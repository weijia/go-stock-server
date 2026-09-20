// go-stock-server/fetcher.go - 股票数据获取（腾讯 HTTP API）
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

// StockFetcher 股票数据获取器
type StockFetcher struct {
	client     *http.Client
	debug      bool
	nameCache  map[string]string
	mu         sync.RWMutex
	klineCache map[string]klineCacheEntry
	klineMu    sync.RWMutex
}

// NewStockFetcher 创建数据获取器
func NewStockFetcher(debug bool) *StockFetcher {
	return &StockFetcher{
		client:     &http.Client{Timeout: 15 * time.Second},
		debug:      debug,
		nameCache:  make(map[string]string),
		klineCache: make(map[string]klineCacheEntry),
	}
}

// isHKCode 港股判定：hk 前缀，或 5 位以内纯数字（如 01810 / 09988）。
// A股/ETF 均为 6 位，不误判；与 Python _is_hk_code 对齐。
func isHKCode(code string) bool {
	c := strings.TrimSpace(strings.ToLower(code))
	if strings.HasPrefix(c, "hk") {
		return true
	}
	if c == "" {
		return false
	}
	for _, r := range c {
		if r < '0' || r > '9' {
			return false
		}
	}
	return len(c) <= 5
}

// apiCode 返回腾讯接口使用的代码：港股去掉 hk 前缀并补零到 5 位（如 hk00700 -> 00700），
// 其余原样返回。
func apiCode(code string) string {
	c := strings.TrimSpace(code)
	if !isHKCode(c) {
		return c
	}
	c = strings.ToLower(c)
	c = strings.TrimPrefix(c, "hk")
	c = strings.TrimSpace(c)
	for len(c) < 5 {
		c = "0" + c
	}
	return c
}

// marketPrefix 根据股票代码判断市场前缀（腾讯接口用：sh / sz / hk）
func marketPrefix(code string) string {
	code = strings.TrimSpace(code)
	if isHKCode(code) {
		return "hk"
	}
	if strings.HasPrefix(code, "6") || strings.HasPrefix(code, "5") ||
		strings.HasPrefix(code, "9") {
		return "sh"
	}
	return "sz"
}

// FetchRealtime 获取实时行情
func (f *StockFetcher) FetchRealtime(code string) (*RealtimeData, error) {
	prefix := marketPrefix(code)
	sym := apiCode(code)
	url := fmt.Sprintf("http://qt.gtimg.cn/q=%s%s", prefix, sym)

	body, err := f.httpGet(url)
	if err != nil {
		return nil, fmt.Errorf("请求实时行情失败: %w", err)
	}

	return f.parseRealtime(body, code)
}

// K 线缓存（对齐 Python _kline_cache）：新鲜期内直接返回；全部源失败回退 stale TTL 内旧值。
const (
	klineCacheFreshTTL = 60  // 秒：新鲜期，命中即返回不回源
	klineCacheStaleTTL = 600 // 秒：全部源失败时回退此期内的旧值（对齐 Python _cache_stale_ttl）
)

type klineCacheEntry struct {
	ts   int64
	resp *KlineResponse
}

// FetchKline 获取K线数据（多源兜底，对齐 Python fetch_kline_qfq 主链）。
//
// 优先级：① 缓存新鲜命中 → ② 网络源 腾讯A股 → 新浪A股 → 东财A股（任一成功即用）
// → ③ 全部源失败回退 stale TTL 内旧缓存（标记 stale）。港股走腾讯当日快照单源。
//
// 之所以多源：线上曾出现腾讯 web.ifzq.gtimg.cn fqkline 被 WAF 拦截（HTTP 501 HTML），
// 单源时直接 503；新浪/东财为不同域名，可绕过该拦截（对齐 Python 多网络源策略）。
func (f *StockFetcher) FetchKline(code string, days int) (*KlineResponse, string, bool, error) {
	// 港股：腾讯 fqkline 接口不提供前复权历史（无 qfqday 字段），
	// 与 Python _qfq_from_hk_tencent 一致改走当日快照构造 1 行未复权数据。
	if isHKCode(code) {
		resp, err := f.fetchHKKline(code)
		return resp, "腾讯HTTP(港股快照)", false, err
	}

	cacheKey := fmt.Sprintf("%s:%d", code, days)
	now := time.Now().Unix()

	// ① 新鲜缓存命中
	if cached, ok := f.klineCacheGet(cacheKey); ok && now-cached.ts < klineCacheFreshTTL {
		return cached.resp, "缓存", false, nil
	}

	prefix := marketPrefix(code)
	sym := apiCode(code)
	sources := []struct {
		name  string
		fetch func() (*KlineResponse, error)
	}{
		{"腾讯A股", func() (*KlineResponse, error) { return f.fetchKlineTencent(prefix, sym, days) }},
		{"新浪A股", func() (*KlineResponse, error) { return f.fetchKlineSina(code, days) }},
		{"东财A股", func() (*KlineResponse, error) { return f.fetchKlineEastMoney(code, days) }},
	}

	var lastErr error
	for _, s := range sources {
		resp, err := s.fetch()
		if err == nil && resp != nil && resp.Count > 0 {
			f.klineCacheSet(cacheKey, resp)
			f.setNameCache(code, resp.Name)
			return resp, s.name, false, nil
		}
		if err != nil {
			lastErr = err
			log.Printf("[KLINE] %s 失败 [%s]: %v", s.name, code, err)
		}
	}

	// ③ 全部源失败 → 回退 stale TTL 内旧缓存（对齐 Python 600s 陈旧兜底，标记 stale）
	if cached, ok := f.klineCacheGet(cacheKey); ok && now-cached.ts < klineCacheStaleTTL {
		log.Printf("[KLINE] 全部源失败，回退 %ds 内缓存(陈旧) [%s]", int(now-cached.ts), code)
		return cached.resp, "缓存(陈旧)", true, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("K线数据获取失败")
	}
	return nil, "", false, lastErr
}

// fetchKlineTencent 腾讯 fqkline（原单源逻辑，含 data:[] 节流退避重试）。
func (f *StockFetcher) fetchKlineTencent(prefix, sym string, days int) (*KlineResponse, error) {
	code := prefix + sym
	// 注意：线上 `web.ifzq.gtimg.cn` 该接口被 WAF 拦截（HTTP 501），改用未被拦的
	// `ifzq.gtimg.cn`（同份 qfq 数据，且支持 ETF/指数），对齐 Python 腾讯子源可用性。
	url := fmt.Sprintf("https://ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s%s,day,,,%d,qfq", prefix, sym, days)
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		body, err := f.httpGet(url)
		if err != nil {
			return nil, fmt.Errorf("请求K线数据失败: %w", err)
		}
		resp, err := f.parseKline(body, code, days)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		// 腾讯对连续/高频请求间歇返回 data:[] 做节流，退避重试自愈（400ms→800ms）。
		if strings.Contains(err.Error(), "K线数据未找到") && attempt < 2 {
			time.Sleep(400 * time.Millisecond * time.Duration(1<<uint(attempt)))
			continue
		}
		return nil, err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("K线数据获取失败")
	}
	return nil, lastErr
}

// fetchKlineSina 新浪 A股前复权日 K（money.finance.sina.com.cn，独立于腾讯域名，
// 用于绕过腾讯 fqkline 被 WAF 拦截的场景，对齐 Python 新浪A股源）。
func (f *StockFetcher) fetchKlineSina(code string, days int) (*KlineResponse, error) {
	prefix := marketPrefix(code)
	sym := apiCode(code)
	// quotes.sina.cn 对 A股/ETF 均可用；money.finance.sina.com.cn 对 ETF 返回 456，故用前者。
	url := fmt.Sprintf("https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketData.getKLineData?symbol=%s%s&scale=240&ma=no&datalen=%d", prefix, sym, days)
	body, err := f.httpGetWithHeaders(url, map[string]string{
		"Referer": "https://finance.sina.com.cn/",
	}, 8*time.Second)
	if err != nil {
		return nil, fmt.Errorf("新浪K线请求失败: %w", err)
	}
	return f.parseKlineSina(body, code, days)
}

func (f *StockFetcher) parseKlineSina(content, code string, days int) (*KlineResponse, error) {
	var rows []struct {
		Day    string `json:"day"`
		Open   string `json:"open"`
		High   string `json:"high"`
		Low    string `json:"low"`
		Close  string `json:"close"`
		Volume string `json:"volume"`
	}
	if err := json.Unmarshal([]byte(content), &rows); err != nil {
		return nil, fmt.Errorf("新浪K线解析失败: %w", err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("新浪K线数据未找到 %s", code)
	}
	records := make([]KlineRecord, 0, len(rows))
	for _, r := range rows {
		date := r.Day
		if i := strings.Index(date, " "); i >= 0 {
			date = date[:i]
		}
		records = append(records, KlineRecord{
			Date:   date,
			Open:   parseFloatSafe(r.Open),
			Close:  parseFloatSafe(r.Close),
			High:   parseFloatSafe(r.High),
			Low:    parseFloatSafe(r.Low),
			Volume: parseFloatSafe(r.Volume),
		})
	}
	if len(records) > days {
		records = records[len(records)-days:]
	}
	return &KlineResponse{Code: code, Name: f.getNameCache(code), Count: len(records), Data: records}, nil
}

// fetchKlineEastMoney 东方财富 K 线（push2his.eastmoney.com，独立于腾讯域名）。
// 本环境东财 https 可能被掐，作为额外兜底；失败不影响腾讯/新浪主源。
func (f *StockFetcher) fetchKlineEastMoney(code string, days int) (*KlineResponse, error) {
	secid := emSecid(code)
	// klt=101 日线；fqt=1 前复权（对齐 qfq 契约）；fields2 取 日期/开/收/高/低/量。
	// 注：本网络 push2his 常被掐，仅作可达时的额外兜底。
	url := fmt.Sprintf("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=%s&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58&klt=101&fqt=1&forcect=1&beg=0&end=20500101", secid)
	body, err := f.httpGetWithHeaders(url, map[string]string{
		"Referer": "https://fund.eastmoney.com/",
	}, 8*time.Second)
	if err != nil {
		return nil, fmt.Errorf("东财K线请求失败: %w", err)
	}
	return f.parseKlineEastMoney(body, code, days)
}

func (f *StockFetcher) parseKlineEastMoney(content, code string, days int) (*KlineResponse, error) {
	var root struct {
		Data *struct {
			Klines []string `json:"klines"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(content), &root); err != nil {
		return nil, fmt.Errorf("东财K线解析失败: %w", err)
	}
	if root.Data == nil || len(root.Data.Klines) == 0 {
		return nil, fmt.Errorf("东财K线数据未找到 %s", code)
	}
	records := make([]KlineRecord, 0, len(root.Data.Klines))
	for _, kl := range root.Data.Klines {
		parts := strings.Split(kl, ",")
		if len(parts) < 6 {
			continue
		}
		records = append(records, KlineRecord{
			Date:   parts[0],
			Open:   parseFloatSafe(parts[1]),
			Close:  parseFloatSafe(parts[2]),
			High:   parseFloatSafe(parts[3]),
			Low:    parseFloatSafe(parts[4]),
			Volume: parseFloatSafe(parts[5]),
		})
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("东财K线解析为空 %s", code)
	}
	if len(records) > days {
		records = records[len(records)-days:]
	}
	return &KlineResponse{Code: code, Name: f.getNameCache(code), Count: len(records), Data: records}, nil
}

// emSecid 东财 secid：沪市 1.xxxxxx，深市 0.xxxxxx。
func emSecid(code string) string {
	c := strings.TrimSpace(code)
	if strings.HasPrefix(c, "6") || strings.HasPrefix(c, "5") || strings.HasPrefix(c, "9") {
		return "1." + c
	}
	return "0." + c
}

// httpGetWithHeaders 带自定义请求头与超时的 GET（供新浪/东财等非腾讯源使用）。
func (f *StockFetcher) httpGetWithHeaders(rawURL string, headers map[string]string, timeout time.Duration) (string, error) {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// klineCacheGet / klineCacheSet 进程内 K 线缓存（读写分离锁）。
func (f *StockFetcher) klineCacheGet(key string) (klineCacheEntry, bool) {
	f.klineMu.RLock()
	defer f.klineMu.RUnlock()
	e, ok := f.klineCache[key]
	return e, ok
}

func (f *StockFetcher) klineCacheSet(key string, resp *KlineResponse) {
	f.klineMu.Lock()
	defer f.klineMu.Unlock()
	f.klineCache[key] = klineCacheEntry{ts: time.Now().Unix(), resp: resp}
}

// fetchHKKline 港股 K 线：腾讯当日快照（qt.gtimg.cn/q=hkXXXXX）构造 1 行未复权数据。
// 与 Python _qfq_from_hk_tencent 对齐：date=今天，open=vals[5]，close=vals[3]，
// high/low 字段充足时取 vals[33]/[34]，否则用 open/close 边界近似；volume 取股数不 ×100。
func (f *StockFetcher) fetchHKKline(code string) (*KlineResponse, error) {
	sym := apiCode(code)
	url := fmt.Sprintf("http://qt.gtimg.cn/q=hk%s", sym)

	body, err := f.httpGet(url)
	if err != nil {
		return nil, fmt.Errorf("请求港股快照失败: %w", err)
	}

	key := fmt.Sprintf("v_hk%s=", sym)
	idx := strings.Index(body, key)
	if idx < 0 {
		return nil, fmt.Errorf("未找到港股 %s 数据", code)
	}
	start := idx + len(key) + 1
	end := strings.Index(body[start:], "\"")
	if end < 0 {
		return nil, fmt.Errorf("解析响应格式失败")
	}
	vals := strings.Split(body[start:start+end], "~")
	if len(vals) < 6 {
		return nil, fmt.Errorf("腾讯返回字段不足(%d)", len(vals))
	}

	price := parseFloatSafe(vals[3])
	if price <= 0 {
		return nil, fmt.Errorf("腾讯返回价格为0(停牌/无数据)")
	}
	open := parseFloatSafe(vals[5])
	var high, low, vol float64
	if len(vals) > 34 {
		high = parseFloatSafe(vals[33])
		low = parseFloatSafe(vals[34])
		vol = parseFloatSafe(vals[36])
	} else {
		high = math.Max(open, price)
		low = math.Min(open, price)
		vol = parseFloatSafe(vals[6])
	}

	name := vals[1]
	if name == "" {
		name = code
	}
	f.setNameCache(code, name)

	records := []KlineRecord{{
		Date:   time.Now().Format("2006-01-02"),
		Open:   open,
		Close:  price,
		High:   high,
		Low:    low,
		Volume: vol,
	}}

	return &KlineResponse{
		Code:  code,
		Name:  f.getNameCache(code),
		Count: len(records),
		Data:  records,
	}, nil
}

// FetchIntraday 获取分时数据
func (f *StockFetcher) FetchIntraday(code string, date string) (*IntradayResponse, error) {
	// 使用腾讯分时 API
	prefix := marketPrefix(code)
	sym := apiCode(code)
	url := fmt.Sprintf("http://ifzq.gtimg.cn/appstock/app/minute/query?_var=min_data&code=%s%s", prefix, sym)

	body, err := f.httpGet(url)
	if err != nil {
		return nil, fmt.Errorf("请求分时数据失败: %w", err)
	}

	return f.parseIntraday(body, code, date)
}

// httpGet HTTP GET 请求
func (f *StockFetcher) httpGet(url string) (string, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

	if f.debug {
		log.Printf("[HTTP] GET %s", url)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		// 瞬时网络错误（DNS 解析失败 / 连接被拒等）自动重试一次，
		// 避免上游域名短暂抖动时整个请求直接失败。
		time.Sleep(400 * time.Millisecond)
		if f.debug {
			log.Printf("[HTTP] 重试 %s (首次失败: %v)", url, err)
		}
		resp, err = f.client.Do(req)
		if err != nil {
			return "", err
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// 腾讯实时行情接口 qt.gtimg.cn 返回 GBK 编码，需转 UTF-8；K线接口为 UTF-8 JSON。
	if strings.Contains(url, "qt.gtimg.cn") {
		return gbkToUtf8(body), nil
	}
	return string(body), nil
}

// gbkToUtf8 将 GBK 字节流转为 UTF-8 字符串（解析失败时回退原字节）
func gbkToUtf8(b []byte) string {
	reader := transform.NewReader(bytes.NewReader(b), simplifiedchinese.GBK.NewDecoder())
	out, err := io.ReadAll(reader)
	if err != nil {
		return string(b)
	}
	return string(out)
}

// parseRealtime 解析实时行情
func (f *StockFetcher) parseRealtime(content, code string) (*RealtimeData, error) {
	key := fmt.Sprintf("v_%s%s=", marketPrefix(code), apiCode(code))

	idx := strings.Index(content, key)
	if idx < 0 {
		return nil, fmt.Errorf("未找到股票 %s 数据", code)
	}

	start := idx + len(key) + 1
	end := strings.Index(content[start:], "\"")
	if end < 0 {
		return nil, fmt.Errorf("解析响应格式失败")
	}
	raw := content[start : start+end]
	parts := strings.Split(raw, "~")
	if len(parts) < 40 {
		return nil, fmt.Errorf("响应字段不足")
	}

	name := parts[1]
	if name == "" {
		name = code
	}
	f.setNameCache(code, name)

	volume := parseFloatSafe(parts[6])
	amount := parseFloatSafe(parts[37])
	if !isHKCode(code) {
		volume *= 100    // 手转股
		amount *= 10000  // 万元转元
	}

	return &RealtimeData{
		Name:      name,
		Code:      code,
		Price:     parseFloatSafe(parts[3]),
		LastClose: parseFloatSafe(parts[4]),
		Open:      parseFloatSafe(parts[5]),
		High:      parseFloatSafe(parts[33]),
		Low:       parseFloatSafe(parts[34]),
		Volume:    volume,
		Amount:    amount,
		ChangeAmt: parseFloatSafe(parts[31]),
		ChangePct: parseFloatSafe(parts[32]),
	}, nil
}

// parseKline 解析 K 线数据（标准 JSON 解码，覆盖 qfqday / day 两种字段）
func (f *StockFetcher) parseKline(content, code string, days int) (*KlineResponse, error) {
	key := fmt.Sprintf("%s%s", marketPrefix(code), apiCode(code))

	var root struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(content), &root); err != nil {
		return nil, fmt.Errorf("K线 JSON 解析失败: %w", err)
	}

	// 腾讯对无效代码/参数错误返回 data:[]（空数组）或 data:null，而非
	// 「代码->K线数据」对象；这类情况归一为明确的业务错误，避免误报 JSON 解析失败。
	raw := bytes.TrimSpace(root.Data)
	if len(raw) == 0 || string(raw) == "null" || raw[0] == '[' {
		// 腾讯对无效代码返回 data:[]；对连续/高频请求（如批量拉 K 线）也会间歇返回
		// 空数组做节流。统一归一为明确的业务错误，由 FetchKline 短暂重试自愈。
		return nil, fmt.Errorf("K线数据未找到 %s（无效代码或接口暂无该股票K线）", code)
	}

	var dataMap map[string]struct {
		QfqDay [][]interface{} `json:"qfqday"`
		Day    [][]interface{} `json:"day"`
	}
	if err := json.Unmarshal(raw, &dataMap); err != nil {
		return nil, fmt.Errorf("K线数据解析失败: %w", err)
	}

	node, ok := dataMap[key]
	if !ok {
		node, ok = dataMap[code]
		if !ok {
			return nil, fmt.Errorf("K线数据未找到 %s", code)
		}
	}

	rows := node.QfqDay
	if len(rows) == 0 {
		rows = node.Day
	}

	var records []KlineRecord
	for _, item := range rows {
		if len(item) < 6 {
			continue
		}
		date, _ := item[0].(string)
		records = append(records, KlineRecord{
			Date:   date,
			Open:   toFloat(item[1]),
			Close:  toFloat(item[2]),
			High:   toFloat(item[3]),
			Low:    toFloat(item[4]),
			Volume: toFloat(item[5]),
		})
	}

	if len(records) > days {
		records = records[len(records)-days:]
	}

	return &KlineResponse{
		Code:  code,
		Name:  f.getNameCache(code),
		Count: len(records),
		Data:  records,
	}, nil
}

// extractJSONArray 从 JSON 字符串中提取指定数组
func extractJSONArray(content, code, field string) []interface{} {
	// 简化的 JSON 解析 - 寻找 "code": [...]
	searchKey := fmt.Sprintf("\"%s\"", code)
	idx := strings.Index(content, searchKey)
	if idx < 0 {
		return nil
	}

	fieldKey := fmt.Sprintf("\"%s\":[", field)
	fieldIdx := strings.Index(content[idx:], fieldKey)
	if fieldIdx < 0 {
		return nil
	}

	start := idx + fieldIdx + len(fieldKey)
	if start >= len(content) {
		return nil
	}

	// 手动解析数组
	var result []interface{}
	depth := 0
	currentStart := -1
	i := start

	for i < len(content) && depth >= 0 {
		c := content[i]
		switch c {
		case '[':
			depth++
			if depth == 1 {
				currentStart = i + 1
			}
		case ']':
			depth--
			if depth == 0 {
				result = append(result, parseArrayElement(content[currentStart:i]))
			}
		case ',':
			if depth == 1 {
				result = append(result, parseArrayElement(content[currentStart:i]))
				currentStart = i + 1
			}
		case '"':
			// skip string
			i++
			for i < len(content) && content[i] != '"' {
				if content[i] == '\\' {
					i++
				}
				i++
			}
		}
		i++
	}

	return result
}

// parseArrayElement 解析数组元素
func parseArrayElement(s string) interface{} {
	s = strings.TrimSpace(s)
	if s == "" || s == "null" {
		return nil
	}
	if strings.HasPrefix(s, "\"") {
		return strings.Trim(s, "\"")
	}
	if strings.HasPrefix(s, "[") {
		// nested array
		var items []interface{}
		inner := s[1 : len(s)-1]
		for _, part := range strings.Split(inner, ",") {
			items = append(items, strings.Trim(strings.TrimSpace(part), "\""))
		}
		return items
	}
	return s
}

// parseIntraday 解析分时数据
func (f *StockFetcher) parseIntraday(content, code, date string) (*IntradayResponse, error) {
	key := fmt.Sprintf("%s%s", marketPrefix(code), apiCode(code))

	// 腾讯分时 API 返回 JavaScript 变量赋值
	// 提取 data 部分
	var intradayData []IntradayRecord

	// 寻找分钟数据
	searchKey := fmt.Sprintf("\"%s\":{", key)
	idx := strings.Index(content, searchKey)
	if idx < 0 {
		return nil, fmt.Errorf("未找到分时数据")
	}

	// 处理分时数据
	// 格式大致为: "data":["09:30 10.50 1000", ...]
	dataIdx := strings.Index(content[idx:], "\"data\":[")
	if dataIdx >= 0 {
		dataStart := idx + dataIdx + len("\"data\":[")
		i := dataStart
		for i < len(content) && content[i] != ']' {
			if content[i] == '"' {
				i++
				elemStart := i
				for i < len(content) && content[i] != '"' {
					i++
				}
				elem := content[elemStart:i]
				// 格式: "09:30 10.50 1000" -> time, price, volume
				parts := strings.Fields(elem)
				if len(parts) >= 2 {
					price := parseFloatSafe(parts[1])
					vol := 0.0
					if len(parts) >= 3 {
						vol = parseFloatSafe(parts[2])
					}
					intradayData = append(intradayData, IntradayRecord{
						Time:   parts[0],
						Price:  price,
						Volume: vol,
					})
				}
			}
			i++
		}
	}

	// 获取昨收价和股票名称
	preClose := 0.0
	stockName := f.getNameCache(code)

	// 尝试获取前一天的收盘价
	realtime, err := f.FetchRealtime(code)
	if err == nil && realtime != nil {
		preClose = realtime.LastClose
		if realtime.Name != "" {
			stockName = realtime.Name
		}
	}

	dataDate := time.Now().Format("2006-01-02")
	if date != "" {
		if len(date) == 8 {
			dataDate = fmt.Sprintf("%s-%s-%s", date[0:4], date[4:6], date[6:8])
		}
	}

	// 计算均价
	totalVolume := 0.0
	totalAmount := 0.0
	for _, rec := range intradayData {
		totalVolume += rec.Volume
		totalAmount += rec.Price * rec.Volume
	}
	avgPrice := 0.0
	if totalVolume > 0 {
		avgPrice = totalAmount / totalVolume
	}
	for i := range intradayData {
		intradayData[i].AvgPrice = roundTo(avgPrice, 2)
	}

	return &IntradayResponse{
		StockCode: code,
		Name:      stockName,
		Date:      dataDate,
		Count:     len(intradayData),
		PreClose:  preClose,
		Data:      intradayData,
	}, nil
}

// set/getNameCache 股票名称缓存
func (f *StockFetcher) setNameCache(code, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nameCache[code] = name
}

func (f *StockFetcher) getNameCache(code string) string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if name, ok := f.nameCache[code]; ok {
		return name
	}
	return ""
}

// ---- 数据结构 ----

// RealtimeData 实时行情数据
type RealtimeData struct {
	Name      string  `json:"name"`
	Code      string  `json:"code"`
	Price     float64 `json:"price"`
	LastClose float64 `json:"last_close"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Volume    float64 `json:"volume"`
	Amount    float64 `json:"amount"`
	ChangeAmt float64 `json:"change_amt"`
	ChangePct float64 `json:"change_pct"`
}

// KlineRecord K线记录
type KlineRecord struct {
	Date   string  `json:"date"`
	Open   float64 `json:"open"`
	Close  float64 `json:"close"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Volume float64 `json:"volume"`
}

// KlineResponse K线响应
type KlineResponse struct {
	Code  string        `json:"code"`
	Name  string        `json:"name"`
	Count int           `json:"count"`
	Data  []KlineRecord `json:"data"`
}

// IntradayRecord 分时记录
type IntradayRecord struct {
	Time     string  `json:"time"`
	Price    float64 `json:"price"`
	Volume   float64 `json:"volume"`
	AvgPrice float64 `json:"avg_price"`
}

// IntradayResponse 分时响应
type IntradayResponse struct {
	StockCode string           `json:"stock_code"`
	Name      string           `json:"name"`
	Date      string           `json:"date"`
	Count     int              `json:"count"`
	PreClose  float64          `json:"pre_close"`
	Data      []IntradayRecord `json:"data"`
}

// ---- 工具函数 ----

func parseFloatSafe(s string) float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0
	}
	return v
}

func toFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case string:
		return parseFloatSafe(val)
	}
	return 0
}

func roundTo(v float64, decimals int) float64 {
	format := fmt.Sprintf("%%.%df", decimals)
	s := fmt.Sprintf(format, v)
	result, _ := strconv.ParseFloat(s, 64)
	return result
}

// ---- 批量 / 名称 / 前复权 ----

// QuoteRecord 批量实时行情单条记录（与 Python /api/batch/quotes 契约一致）
type QuoteRecord struct {
	Code        string  `json:"code"`
	Name        string  `json:"name"`
	Price       float64 `json:"price"`
	Open        float64 `json:"open"`
	PrevClose   float64 `json:"prev_close"`
	High        float64 `json:"high"`
	Low         float64 `json:"low"`
	Volume      float64 `json:"volume"`
	Amount      float64 `json:"amount"`
	PriceSource string  `json:"price_source,omitempty"` // ""=实时, "last_close", "close"
	Stale       bool    `json:"stale"`
	PriceTS     int64   `json:"price_ts"` // 数据取到时刻（unix 秒）
}

// MinuteResponse 分钟 K 线（原始 OHLC 棒），供分时检测复用
type MinuteResponse struct {
	Code  string        `json:"code"`
	Count int           `json:"count"`
	Data  []KlineRecord `json:"data"`
}

// FetchName 获取股票名称（腾讯 HTTP，复用 realtime 解析）
func (f *StockFetcher) FetchName(code string) (string, error) {
	rt, err := f.FetchRealtime(code)
	if err != nil || rt == nil {
		return "", fmt.Errorf("获取股票名称失败 [%s]: %w", code, err)
	}
	return rt.Name, nil
}

// FetchBatchQuotes 批量获取实时行情（腾讯 HTTP：一次取多只收盘价/实时价）
// 返回键为 6 位代码。price_source 默认标 "close"（TDX 优先生效时由调用方覆盖）。
func (f *StockFetcher) FetchBatchQuotes(codes []string) (map[string]*QuoteRecord, error) {
	if len(codes) == 0 {
		return nil, fmt.Errorf("codes 为空")
	}
	parts := make([]string, 0, len(codes))
	for _, c := range codes {
		parts = append(parts, marketPrefix(c)+apiCode(c))
	}
	url := fmt.Sprintf("http://qt.gtimg.cn/q=%s", strings.Join(parts, ","))
	body, err := f.httpGet(url)
	if err != nil {
		return nil, fmt.Errorf("请求批量行情失败: %w", err)
	}
	return f.parseBatchQuotes(body, codes)
}

func (f *StockFetcher) parseBatchQuotes(content string, codes []string) (map[string]*QuoteRecord, error) {
	result := make(map[string]*QuoteRecord)
	now := time.Now().Unix()
	for _, c := range codes {
		marker := fmt.Sprintf("v_%s%s=", marketPrefix(c), apiCode(c))
		idx := strings.Index(content, marker)
		if idx < 0 {
			continue
		}
		rest := content[idx+len(marker):]
		if strings.HasPrefix(rest, "\"") {
			rest = rest[1:]
		}
		end := strings.Index(rest, "\"")
		if end < 0 {
			continue
		}
		raw := rest[:end]
		fields := strings.Split(raw, "~")
		if len(fields) < 40 {
			continue
		}
		name := fields[1]
		if name == "" {
			name = c
		}
		f.setNameCache(c, name)
		vol := parseFloatSafe(fields[6])
		amt := parseFloatSafe(fields[37])
		if !isHKCode(c) {
			vol *= 100    // 手转股
			amt *= 10000  // 万元转元
		}
		result[c] = &QuoteRecord{
			Code:      c,
			Name:      name,
			Price:     parseFloatSafe(fields[3]),
			PrevClose: parseFloatSafe(fields[4]),
			Open:      parseFloatSafe(fields[5]),
			High:      parseFloatSafe(fields[33]),
			Low:       parseFloatSafe(fields[34]),
			Volume:    vol,
			Amount:    amt,
			PriceSource: "close",
			Stale:    false,
			PriceTS:  now,
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("未解析到任何行情")
	}
	return result, nil
}

// FetchQfq 前复权日 K 线（多源，对齐 Python fetch_kline_qfq）。
// 返回 K线响应、数据源标识、是否未复权兜底、是否陈旧缓存、错误。
func (f *StockFetcher) FetchQfq(code string, days int) (*KlineResponse, string, bool, bool, error) {
	resp, source, stale, err := f.FetchKline(code, days)
	if err != nil {
		return nil, "", false, stale, err
	}
	if resp == nil || resp.Count == 0 {
		return nil, "", false, stale, fmt.Errorf("前复权K线无数据 [%s]", code)
	}
	return resp, source, false, stale, nil
}
