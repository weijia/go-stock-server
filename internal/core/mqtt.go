// go-stock-server/mqtt.go - MQTT 接入层（实时价推送 + 行情命令响应）
//
// 与 Python mqtt_protocol.py 对齐：OpenSSL 兼容 AES 加密、信封/msgId 去重、
// 自消息过滤、ping-pong、subscribe/unsubscribe/list_subs、quote_push 推送、
// 行情命令 (realtime/batch/kline/qfq/minute/intraday/name/quote_cache/health/config)。
package core

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// ── 命令执行器 ──────────────────────────────────────────────────────────────

func okResp(action string, data interface{}, code ...string) map[string]interface{} {
	m := map[string]interface{}{"action": action, "status": "success", "data": data}
	if len(code) > 0 {
		m["code"] = code[0]
	}
	return m
}

func errResp(action, message string, code ...string) map[string]interface{} {
	m := map[string]interface{}{"action": action, "status": "error", "message": message}
	if len(code) > 0 {
		m["code"] = code[0]
	}
	return m
}

func strval(v interface{}) string {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	case float64:
		return strconv.FormatInt(int64(x), 10)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", x)
	}
}

func intval(v interface{}, def int) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case string:
		if n, err := strconv.Atoi(x); err == nil {
			return n
		}
	}
	return def
}

func parseCodes(raw interface{}) []string {
	out := make([]string, 0)
	switch v := raw.(type) {
	case string:
		for _, c := range strings.Split(v, ",") {
			c = strings.TrimSpace(c)
			if c != "" {
				out = append(out, c)
			}
		}
	case []interface{}:
		for _, item := range v {
			s := strval(item)
			if s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

// mqttCommandHandler 行情命令执行器（复用 handler 的 Provider/缓存，与 HTTP 同源）
type mqttCommandHandler struct {
	h *StockHandler
}

func (ch *mqttCommandHandler) execute(action string, data map[string]interface{}) map[string]interface{} {
	switch action {
	case "realtime":
		return ch.realtime(data)
	case "batch":
		return ch.batch(data)
	case "kline":
		return ch.kline(data)
	case "qfq":
		return ch.qfq(data)
	case "minute":
		return ch.minute(data)
	case "intraday":
		return ch.intraday(data)
	case "name":
		return ch.name(data)
	case "quote_cache":
		return ch.quoteCache(data)
	case "health":
		return ch.health()
	case "config":
		return ch.config()
	default:
		return errResp(action, fmt.Sprintf("未知命令: %s", action))
	}
}

func (ch *mqttCommandHandler) realtime(data map[string]interface{}) map[string]interface{} {
	code := strval(data["code"])
	if code == "" {
		return errResp("realtime", "缺少 code")
	}
	var rec *RealtimeData
	var err error
	if ch.h.tdx != nil && ch.h.tdx.Enabled() {
		rec, err = ch.h.tdx.GetRealtime(code)
	}
	if rec == nil {
		rec, err = ch.h.fetcher.FetchRealtime(code)
	}
	if rec == nil {
		return errResp("realtime", fmt.Sprintf("无可用数据源: %v", err), code)
	}
	return okResp("realtime", map[string]interface{}{
		"name":       rec.Name,
		"code":       code,
		"price":      rec.Price,
		"open":       rec.Open,
		"prev_close": rec.LastClose,
		"high":       rec.High,
		"low":        rec.Low,
		"change_pct": rec.ChangePct,
	}, code)
}

func (ch *mqttCommandHandler) batch(data map[string]interface{}) map[string]interface{} {
	codes := parseCodes(data["codes"])
	if len(codes) == 0 {
		return errResp("batch", "缺少 codes")
	}
	result := ch.h.fetchBatchQuotes(codes)
	if len(result) == 0 {
		return errResp("batch", "无可用数据源")
	}
	ch.h.quoteCache.UpsertMany(result)
	anyStale := false
	for _, r := range result {
		if r.Stale {
			anyStale = true
		}
	}
	return map[string]interface{}{
		"action": "batch",
		"status": "success",
		"data":   map[string]interface{}{"data": result, "stale": anyStale},
	}
}

func (ch *mqttCommandHandler) kline(data map[string]interface{}) map[string]interface{} {
	code := strval(data["code"])
	if code == "" {
		return errResp("kline", "缺少 code")
	}
	days := intval(data["days"], 30)
	resp, err := ch.h.fetcher.FetchKline(code, days)
	if err != nil || resp == nil || resp.Count == 0 {
		return errResp("kline", fmt.Sprintf("无可用数据源: %v", err), code)
	}
	return okResp("kline", map[string]interface{}{
		"code":    code,
		"name":    resp.Name,
		"count":   resp.Count,
		"data":    resp.Data,
		"stale":   false,
		"price_ts": time.Now().Format(time.RFC3339),
	}, code)
}

func (ch *mqttCommandHandler) qfq(data map[string]interface{}) map[string]interface{} {
	code := strval(data["code"])
	if code == "" {
		return errResp("qfq", "缺少 code")
	}
	days := intval(data["days"], 30)
	resp, source, unadjusted, err := ch.h.fetcher.FetchQfq(code, days)
	if err != nil || resp == nil || resp.Count == 0 {
		return errResp("qfq", fmt.Sprintf("无可用数据源: %v", err), code)
	}
	resp = ch.h.ensureTodayOpen(resp, code)
	return okResp("qfq", map[string]interface{}{
		"code":       code,
		"name":       resp.Name,
		"count":      resp.Count,
		"data":       resp.Data,
		"stale":      false,
		"price_ts":   time.Now().Format(time.RFC3339),
		"source":     source,
		"unadjusted": unadjusted,
	}, code)
}

func (ch *mqttCommandHandler) minute(data map[string]interface{}) map[string]interface{} {
	code := strval(data["code"])
	if code == "" {
		return errResp("minute", "缺少 code")
	}
	period := intval(data["period"], 7)
	minutes := intval(data["minutes"], 300)
	var d *MinuteResponse
	var err error
	if ch.h.tdx != nil && ch.h.tdx.Enabled() {
		d, err = ch.h.tdx.GetMinute(code, period, minutes)
	}
	if err != nil || d == nil || d.Count == 0 {
		return errResp("minute", fmt.Sprintf("无可用分钟数据源: %v", err), code)
	}
	return okResp("minute", map[string]interface{}{
		"code":  code,
		"count": d.Count,
		"data":  d.Data,
	}, code)
}

func (ch *mqttCommandHandler) intraday(data map[string]interface{}) map[string]interface{} {
	code := strval(data["code"])
	if code == "" {
		return errResp("intraday", "缺少 code")
	}
	var d *MinuteResponse
	var err error
	if ch.h.tdx != nil && ch.h.tdx.Enabled() {
		d, err = ch.h.tdx.GetMinute(code, 7, 240)
	}
	if err != nil || d == nil || d.Count == 0 {
		return errResp("intraday", "分时数据不可用，请确保通达信已下载当天数据", code)
	}
	// 由 1 分钟 K 线构造分时（与 Python mqtt intraday 策略1 对齐）
	intradayData := make([]map[string]interface{}, 0, len(d.Data))
	totalVolume := 0.0
	totalAmount := 0.0
	dateSet := map[string]bool{}
	for _, r := range d.Data {
		price := r.Close
		vol := r.Volume
		totalVolume += vol
		totalAmount += price * vol
		tm := ""
		if len(r.Date) >= 12 {
			tm = r.Date[8:10] + ":" + r.Date[10:12]
		} else if len(r.Date) >= 8 {
			tm = "15:00"
		}
		dayPart := r.Date
		if len(r.Date) >= 10 {
			dayPart = r.Date[:10]
		} else if len(r.Date) >= 8 {
			dayPart = r.Date[0:4] + "-" + r.Date[4:6] + "-" + r.Date[6:8]
		}
		dateSet[dayPart] = true
		intradayData = append(intradayData, map[string]interface{}{
			"time":  tm,
			"price": price,
			"volume": vol,
		})
	}
	avg := 0.0
	if totalVolume > 0 {
		avg = totalAmount / totalVolume
	}
	for _, item := range intradayData {
		item["avg_price"] = roundTo(avg, 2)
	}
	name := ""
	preClose := 0.0
	if rt, e := ch.h.fetcher.FetchRealtime(code); e == nil && rt != nil {
		name = rt.Name
		preClose = rt.LastClose
	}
	dataDate := time.Now().Format("2006-01-02")
	if len(dateSet) > 0 {
		ds := make([]string, 0, len(dateSet))
		for k := range dateSet {
			ds = append(ds, k)
		}
		sort.Strings(ds)
		dataDate = ds[len(ds)-1]
	}
	return okResp("intraday", map[string]interface{}{
		"stock_code": code,
		"name":       name,
		"date":       dataDate,
		"count":      len(intradayData),
		"pre_close":  preClose,
		"data":       intradayData,
	}, code)
}

func (ch *mqttCommandHandler) name(data map[string]interface{}) map[string]interface{} {
	code := strval(data["code"])
	if code == "" {
		return errResp("name", "缺少 code")
	}
	name, _ := ch.h.fetcher.FetchName(code)
	return okResp("name", map[string]interface{}{"code": code, "name": name}, code)
}

func (ch *mqttCommandHandler) quoteCache(data map[string]interface{}) map[string]interface{} {
	codes := parseCodes(data["codes"])
	out := map[string]interface{}{}
	if len(codes) > 0 {
		for _, c := range codes {
			if rec := ch.h.quoteCache.Get(c); rec != nil {
				out[rec.Code] = rec
			}
		}
	} else {
		for k, v := range ch.h.quoteCache.GetAll() {
			out[k] = v
		}
	}
	return okResp("quote_cache", out)
}

func (ch *mqttCommandHandler) health() map[string]interface{} {
	up := int(time.Since(serverStartTime).Seconds())
	hours := up / 3600
	minutes := (up % 3600) / 60
	seconds := up % 60
	return okResp("health", map[string]interface{}{
		"status":         "ok",
		"version":        serverVersion,
		"uptime":         fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds),
		"uptime_seconds": up,
	})
}

func (ch *mqttCommandHandler) config() map[string]interface{} {
	tdxEnabled := ch.h.tdx != nil && ch.h.tdx.Enabled()
	dataSource := "腾讯行情 (Tencent HTTP)"
	if tdxEnabled {
		dataSource = "通达信TCP优先 + 腾讯HTTP回退"
	}
	sources := map[string]interface{}{
		"mootdx_tcp":  tdxEnabled,
		"tencent_http": true,
		"best_server": nil,
	}
	caps := map[string]interface{}{
		"realtime_single":  true,
		"kline_historical": true,
		"intraday_minutes": tdxEnabled,
		"mqtt":             true,
	}
	return okResp("config", map[string]interface{}{
		"server": map[string]interface{}{
			"version":     serverVersion,
			"data_source": dataSource,
		},
		"sources":      sources,
		"capabilities": caps,
		"mqtt_commands": []string{
			"realtime", "batch", "kline", "qfq", "minute", "intraday",
			"name", "quote_cache", "health", "config",
			"subscribe", "unsubscribe", "list_subs",
		},
	})
}

// ── MQTT 客户端 ──────────────────────────────────────────────────────────────

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "000000"
	}
	const hexd = "0123456789abcdef"
	out := make([]byte, n)
	for i, v := range b {
		out[i] = hexd[int(v)%16]
	}
	return string(out)
}

func parseBrokerURL(raw string) (scheme, host string, port int, path string) {
	u, err := url.Parse(raw)
	if err != nil {
		return "mqtt", "broker.emqx.io", 1883, "/mqtt"
	}
	scheme = strings.ToLower(u.Scheme)
	if scheme == "" {
		scheme = "mqtt"
	}
	host = u.Hostname()
	p, _ := strconv.Atoi(u.Port())
	switch scheme {
	case "wss", "ws":
		if p == 0 {
			p = 8084
		}
	case "mqtts", "ssl", "tls":
		if p == 0 {
			p = 8883
		}
	default:
		if p == 0 {
			p = 1883
		}
	}
	path = u.Path
	if path == "" {
		path = "/mqtt"
	}
	return
}

// MQTTPriceClient Price Server 的 MQTT 客户端
type MQTTPriceClient struct {
	handler *StockHandler
	broker  string
	topic   string
	password string
	clientID string
	userSuffix string
	pushInterval int
	pushOnlyTrading bool
	wsPath string

	client    mqtt.Client
	connected bool
	running   bool
	lastPong  int64

	subs     map[string]bool
	subsLock sync.Mutex

	seen     map[string]int64
	seenLock sync.Mutex

	cmd *mqttCommandHandler

	wg sync.WaitGroup
}

// NewMQTTClient 创建 MQTT 客户端
func NewMQTTClient(h *StockHandler, broker, topic, password, clientID, userSuffix string, pushInterval int, pushOnlyTrading bool) *MQTTPriceClient {
	if clientID == "" {
		clientID = "ps_" + randHex(6)
	}
	if pushInterval < 1 {
		pushInterval = 5
	}
	_, _, _, wsPath := parseBrokerURL(broker)
	return &MQTTPriceClient{
		handler:         h,
		broker:          broker,
		topic:           topic,
		password:        password,
		clientID:        clientID,
		userSuffix:      userSuffix,
		pushInterval:    pushInterval,
		pushOnlyTrading: pushOnlyTrading,
		wsPath:          wsPath,
		subs:            map[string]bool{},
		seen:            map[string]int64{},
		cmd:             &mqttCommandHandler{h: h},
	}
}

func (c *MQTTPriceClient) buildOptions() *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.SetClientID(c.clientID)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(2 * time.Second)
	opts.SetMaxReconnectInterval(30 * time.Second)
	opts.SetConnectTimeout(30 * time.Second)
	opts.SetKeepAlive(60 * time.Second)
	opts.SetCleanSession(true)
	opts.SetOnConnectHandler(c.onConnect)
	opts.SetConnectionLostHandler(c.onDisconnect)
	opts.SetDefaultPublishHandler(c.onMessage)
	opts.AddBroker(c.broker)

	scheme, _, _, _ := parseBrokerURL(c.broker)
	if scheme == "wss" || scheme == "ws" {
		opts.SetWebsocketOptions(&mqtt.WebsocketOptions{})
	}
	if scheme == "wss" || scheme == "mqtts" || scheme == "ssl" || scheme == "tls" {
		opts.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	return opts
}

// Start 启动 MQTT 客户端（后台连接 + 推送/心跳/状态线程）
func (c *MQTTPriceClient) Start() bool {
	c.running = true
	go func() {
		for c.running {
			opts := c.buildOptions()
			c.client = mqtt.NewClient(opts)
			token := c.client.Connect()
			token.Wait()
			if token.Error() == nil {
				log.Printf("[MQTT] 已连接 broker=%s topic=%s clientId=%s", c.broker, c.topic, c.clientID)
				return
			}
			log.Printf("[MQTT] 连接失败: %v，5s 后重试", token.Error())
			time.Sleep(5 * time.Second)
		}
	}()
	c.wg.Add(3)
	go c.heartbeatLoop()
	go c.pushLoop()
	go c.statusLoop()
	log.Printf("[MQTT] 接入层已启动（clientId=%s）", c.clientID)
	return true
}

// Stop 停止 MQTT 客户端
func (c *MQTTPriceClient) Stop() {
	c.running = false
	if c.client != nil {
		c.client.Disconnect(250)
	}
	c.wg.Wait()
	log.Println("[MQTT] 已停止")
}

func (c *MQTTPriceClient) onConnect(client mqtt.Client) {
	c.connected = true
	if token := client.Subscribe(c.topic, 0, nil); token.Wait() && token.Error() != nil {
		log.Printf("[MQTT] 订阅失败: %v", token.Error())
	}
	c.sendRaw("ping", "_ping")
	c.sendStatus()
}

func (c *MQTTPriceClient) onDisconnect(client mqtt.Client, err error) {
	c.connected = false
	log.Printf("[MQTT] 连接断开（将自动重连）: %v", err)
}

func (c *MQTTPriceClient) onMessage(client mqtt.Client, msg mqtt.Message) {
	payload := string(msg.Payload())
	plaintext, err := decrypt(payload, c.password)
	if err != nil {
		log.Printf("[MQTT] 解密失败（密钥不匹配?）: %v", err)
		return
	}
	var envelope map[string]interface{}
	if err := json.Unmarshal([]byte(plaintext), &envelope); err != nil {
		log.Printf("[MQTT] 信封不是合法 JSON，丢弃")
		return
	}
	senderID, _ := envelope["id"].(string)
	if senderID == c.clientID {
		return // 自消息过滤
	}
	msgID, _ := envelope["msgId"].(string)
	if msgID != "" {
		now := time.Now().Unix()
		c.seenLock.Lock()
		if t, ok := c.seen[msgID]; ok && (now-t) < 5 {
			c.seenLock.Unlock()
			return // 去重
		}
		c.seen[msgID] = now
		for k, v := range c.seen {
			if now-v > 5 {
				delete(c.seen, k)
			}
		}
		c.seenLock.Unlock()
	}

	msgBody := envelope["msg"]
	var inner map[string]interface{}
	switch v := msgBody.(type) {
	case string:
		if v == "ping" {
			c.sendRaw("pong", "_pong")
			return
		}
		if v == "pong" {
			c.lastPong = time.Now().Unix()
			return
		}
		if err := json.Unmarshal([]byte(v), &inner); err != nil {
			log.Printf("[MQTT] msg 不是合法 JSON，丢弃")
			return
		}
	case map[string]interface{}:
		inner = v
	default:
		return
	}
	if inner == nil {
		return
	}
	action, _ := inner["action"].(string)
	if action == "" {
		return
	}
	if action == "ping" {
		c.sendRaw("pong", "_pong")
		return
	}
	if action == "pong" {
		c.lastPong = time.Now().Unix()
		return
	}

	reqMsgID := msgID
	if im, ok := inner["msgId"].(string); ok && im != "" {
		reqMsgID = im
	}

	if action == "subscribe" || action == "unsubscribe" || action == "list_subs" {
		c.handleSubscription(action, inner, reqMsgID)
		return
	}

	known := map[string]bool{
		"realtime": true, "batch": true, "kline": true, "qfq": true,
		"minute": true, "intraday": true, "name": true, "quote_cache": true,
		"health": true, "config": true,
	}
	if !known[action] {
		log.Printf("[MQTT] 忽略非行情命令 action=%s", action)
		return
	}

	data := map[string]interface{}{}
	if d, ok := inner["data"].(map[string]interface{}); ok {
		data = d
	}
	resp := c.cmd.execute(action, data)
	c.sendResponse(resp, reqMsgID)
}

func (c *MQTTPriceClient) handleSubscription(action string, data map[string]interface{}, reqMsgID string) {
	c.subsLock.Lock()
	switch action {
	case "subscribe":
		codes := parseCodes(data["codes"])
		if len(codes) == 0 {
			c.subsLock.Unlock()
			c.sendResponse(errResp("subscribe", "缺少 codes"), reqMsgID)
			return
		}
		for _, cd := range codes {
			c.subs[cd] = true
		}
		if iv := intval(data["interval"], 0); iv >= 1 {
			c.pushInterval = iv
		}
		subs := c.subsKeys()
		count := len(c.subs)
		c.subsLock.Unlock()
		c.sendResponse(okResp("subscribe", map[string]interface{}{
			"subscribed":    subs,
			"count":         count,
			"push_interval": c.pushInterval,
		}), reqMsgID)
	case "unsubscribe":
		codes := parseCodes(data["codes"])
		if len(codes) == 0 {
			c.subs = map[string]bool{}
		} else {
			for _, cd := range codes {
				delete(c.subs, cd)
			}
		}
		subs := c.subsKeys()
		count := len(c.subs)
		c.subsLock.Unlock()
		c.sendResponse(okResp("unsubscribe", map[string]interface{}{
			"subscribed": subs,
			"count":      count,
		}), reqMsgID)
	case "list_subs":
		subs := c.subsKeys()
		count := len(c.subs)
		c.subsLock.Unlock()
		c.sendResponse(okResp("list_subs", map[string]interface{}{
			"subscribed":    subs,
			"count":         count,
			"push_interval": c.pushInterval,
		}), reqMsgID)
	}
}

func (c *MQTTPriceClient) subsKeys() []string {
	keys := make([]string, 0, len(c.subs))
	for k := range c.subs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (c *MQTTPriceClient) publish(msgContent string, msgID string) {
	if c.client == nil {
		return
	}
	if msgID == "" {
		msgID = fmt.Sprintf("%d_%s", time.Now().UnixMilli(), randHex(6))
	}
	envelope := map[string]interface{}{
		"id":     c.clientID,
		"msgId":  msgID,
		"user":   "price-server" + c.userSuffix,
		"msg":    msgContent,
		"time":   time.Now().UnixMilli(),
	}
	plain, err := json.Marshal(envelope)
	if err != nil {
		return
	}
	cipher, err := encrypt(string(plain), c.password)
	if err != nil {
		log.Printf("[MQTT] 加密失败: %v", err)
		return
	}
	c.client.Publish(c.topic, 0, false, cipher)
}

func (c *MQTTPriceClient) sendRaw(msgContent string, suffix string) {
	c.publish(msgContent, fmt.Sprintf("%d_%s%s", time.Now().UnixMilli(), randHex(6), suffix))
}

func (c *MQTTPriceClient) sendResponse(resp map[string]interface{}, reqMsgID string) {
	inner, err := json.Marshal(map[string]interface{}{"action": "response", "data": resp})
	if err != nil {
		return
	}
	c.publish(string(inner), reqMsgID)
}

func (c *MQTTPriceClient) heartbeatLoop() {
	defer c.wg.Done()
	for c.running {
		time.Sleep(30 * time.Second)
		if !c.running {
			break
		}
		if c.connected {
			c.sendRaw("ping", "_ping")
			if c.lastPong != 0 && (time.Now().Unix()-c.lastPong) > 35 {
				log.Printf("[MQTT] 超过一个心跳周期未收到 pong，远端可能离线")
			}
		}
	}
}

func (c *MQTTPriceClient) pushLoop() {
	defer c.wg.Done()
	for c.running {
		time.Sleep(time.Duration(c.pushInterval) * time.Second)
		if !c.running {
			break
		}
		c.subsLock.Lock()
		codes := c.subsKeys()
		c.subsLock.Unlock()
		if len(codes) == 0 {
			continue
		}
		if c.pushOnlyTrading && !marketOpen() {
			continue
		}
		if c.handler.tdx == nil || !c.handler.tdx.Enabled() {
			continue
		}
		quotes := c.handler.fetchBatchQuotes(codes)
		if len(quotes) == 0 {
			continue
		}
		c.handler.quoteCache.UpsertMany(quotes)
		for _, code := range codes {
			rec, ok := quotes[code]
			if !ok || rec == nil || rec.Price <= 0 {
				continue
			}
			pc := rec.PrevClose
			changePct := 0.0
			if pc > 0 {
				changePct = (rec.Price - pc) / pc * 100
			}
			pushData := map[string]interface{}{
				"code":       code,
				"name":       rec.Name,
				"price":      rec.Price,
				"open":       rec.Open,
				"prev_close": pc,
				"high":       rec.High,
				"low":        rec.Low,
				"change_pct": roundTo(changePct, 4),
				"price_ts":   rec.PriceTS,
				"push_time":  time.Now().Unix(),
				"stale":      rec.Stale,
			}
			inner, _ := json.Marshal(map[string]interface{}{"action": "quote_push", "data": pushData})
			c.publish(string(inner), "")
		}
	}
}

func (c *MQTTPriceClient) statusLoop() {
	defer c.wg.Done()
	for c.running {
		time.Sleep(60 * time.Second)
		if !c.running {
			break
		}
		if c.connected {
			c.sendStatus()
		}
	}
}

func (c *MQTTPriceClient) sendStatus() {
	tdxEnabled := c.handler.tdx != nil && c.handler.tdx.Enabled()
	services := []map[string]interface{}{
		{
			"name":     "mootdx",
			"type":     "tdx-tcp",
			"running":  tdxEnabled,
			"detail":   "通达信TCP",
			"available": true,
		},
		{
			"name":     "tencent",
			"type":     "http",
			"running":  true,
			"detail":   "腾讯行情 HTTP",
			"available": true,
		},
	}
	inner := map[string]interface{}{
		"action": "status",
		"data":   services,
		"extra": map[string]interface{}{
			"version":         serverVersion,
			"uptime_seconds":  int(time.Since(serverStartTime).Seconds()),
		},
	}
	plain, _ := json.Marshal(inner)
	c.publish(string(plain), "")
}

func marketOpen() bool {
	now := time.Now()
	if now.Weekday() >= time.Saturday {
		return false
	}
	t := now.Hour()*60 + now.Minute()
	return (9*60+15 <= t && t <= 11*60+30) || (13*60 <= t && t <= 15*60)
}
