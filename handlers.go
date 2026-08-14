// go-stock-server/handlers.go - HTTP API 处理器
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var serverStartTime = time.Now()
var serverVersion = "2.0.0-go"

// StockHandler HTTP 请求处理器
type StockHandler struct {
	fetcher    *StockFetcher
	tdx        *TdxDataSource
	nodeStore  *NodeConfigStore
	quoteCache *QuoteCache
	debug      bool
}

// NewStockHandler 创建处理器
func NewStockHandler(fetcher *StockFetcher, tdx *TdxDataSource, nodeStore *NodeConfigStore, quoteCache *QuoteCache, debug bool) *StockHandler {
	return &StockHandler{
		fetcher:   fetcher,
		tdx:       tdx,
		nodeStore: nodeStore,
		quoteCache: quoteCache,
		debug:     debug,
	}
}

// logRequest 记录请求
func (h *StockHandler) logRequest(r *http.Request) {
	if !h.debug {
		return
	}
	log.Printf("══════════════════════════════════════════════════════════")
	log.Printf("[REQUEST] %s %s", r.Method, r.URL.Path)
	log.Printf("  客户端: %s", r.RemoteAddr)
	log.Printf("  时间: %s", time.Now().Format("2006-01-02 15:04:05.000"))
	if r.URL.RawQuery != "" {
		log.Printf("  参数: %s", r.URL.RawQuery)
	}
	log.Printf("  Headers:")
	for k, v := range r.Header {
		for _, vv := range v {
			log.Printf("    %s: %s", k, vv)
		}
	}
	log.Printf("══════════════════════════════════════════════════════════")
}

// sendJSON 发送 JSON 响应
func (h *StockHandler) sendJSON(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(data)
}

// getElapsedMS 计算处理耗时
func getElapsedMS(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000.0
}

// getNodeID 从请求头获取节点 ID
func getNodeID(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get("X-Node-ID"))
}

// HandleHealth 健康检查
func (h *StockHandler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.logRequest(r)

	uptime := int(time.Since(serverStartTime).Seconds())
	hours := uptime / 3600
	minutes := (uptime % 3600) / 60
	seconds := uptime % 60

	h.sendJSON(w, 200, map[string]interface{}{
		"code": 200,
		"data": map[string]interface{}{
			"status":         "ok",
			"version":        serverVersion,
			"uptime":         fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds),
			"uptime_seconds": uptime,
		},
		"timestamp": time.Now().Format(time.RFC3339),
	})
	h.logResponse(200, start)
}

// HandleConfig 服务器配置信息
func (h *StockHandler) HandleConfig(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.logRequest(r)

	section := r.URL.Query().Get("section")
	uptime := int(time.Since(serverStartTime).Seconds())

	tdxEnabled := h.tdx != nil && h.tdx.Enabled()
	dataSource := "腾讯行情 (Tencent HTTP)"
	sources := map[string]interface{}{"tencent_http": true}
	if tdxEnabled {
		dataSource = "通达信TCP优先 + 腾讯HTTP回退"
		sources["tdx_tcp"] = true
	}

	fullConfig := map[string]interface{}{
		"server": map[string]interface{}{
			"version":        serverVersion,
			"start_time":     serverStartTime.Format(time.RFC3339),
			"uptime_seconds": uptime,
			"data_source":    dataSource,
		},
		"sources":      sources,
		"tdx_enabled":  tdxEnabled,
		"markets":      []string{"A股", "ETF"},
		"capabilities": map[string]interface{}{
			"realtime_single":     true,
			"kline_historical":    true,
			"intraday_minutes":    true,
			"health_check":        true,
			"config_query":        true,
		},
		"limits": map[string]interface{}{
			"kline_max_days":     500,
			"kline_min_days":     1,
			"intraday_max_minutes": 240,
		},
	}

	responseData := fullConfig
	if section != "" {
		if sec, ok := fullConfig[section]; ok {
			responseData = map[string]interface{}{section: sec}
		}
	}

	h.sendJSON(w, 200, map[string]interface{}{
		"code":      200,
		"data":      responseData,
		"message":   "success",
		"timestamp": time.Now().Format(time.RFC3339),
	})
	h.logResponse(200, start)
}

// HandleNodeConfig 节点配置管理
func (h *StockHandler) HandleNodeConfig(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.logRequest(r)

	switch r.Method {
	case "GET":
		h.handleNodeConfigGet(w, r, start)
	case "POST":
		h.handleNodeConfigPost(w, r, start)
	case "DELETE":
		h.handleNodeConfigDelete(w, r, start)
	default:
		h.sendJSON(w, 405, map[string]interface{}{
			"code":    405,
			"message": "不支持的方法",
		})
	}
}

func (h *StockHandler) handleNodeConfigGet(w http.ResponseWriter, r *http.Request, start time.Time) {
	nodeID := getNodeID(r)
	if nodeID == "" {
		h.sendJSON(w, 400, map[string]interface{}{
			"code":    400,
			"message": "缺少 X-Node-ID 请求头",
		})
		return
	}

	if h.nodeStore == nil {
		h.sendJSON(w, 503, map[string]interface{}{
			"code":    503,
			"message": "节点配置存储不可用",
		})
		return
	}

	config := h.nodeStore.Get(nodeID)
	if config == nil {
		// 首次请求，自动创建默认配置
		config = h.nodeStore.Create(nodeID)
		log.Printf("自动创建节点配置: %s", nodeID)
	}

	h.sendJSON(w, 200, map[string]interface{}{
		"code":      200,
		"data":      config,
		"message":   "success",
		"timestamp": time.Now().Format(time.RFC3339),
	})
	h.logResponse(200, start)
}

func (h *StockHandler) handleNodeConfigPost(w http.ResponseWriter, r *http.Request, start time.Time) {
	nodeID := getNodeID(r)
	if nodeID == "" {
		h.sendJSON(w, 400, map[string]interface{}{
			"code":    400,
			"message": "缺少 X-Node-ID 请求头",
		})
		return
	}

	if h.nodeStore == nil {
		h.sendJSON(w, 503, map[string]interface{}{
			"code":    503,
			"message": "节点配置存储不可用",
		})
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		h.sendJSON(w, 400, map[string]interface{}{
			"code":    400,
			"message": fmt.Sprintf("JSON 格式错误: %v", err),
		})
		return
	}

	config, err := h.nodeStore.Update(nodeID, data)
	if err != nil {
		h.sendJSON(w, 400, map[string]interface{}{
			"code":    400,
			"message": err.Error(),
		})
		return
	}

	h.sendJSON(w, 200, map[string]interface{}{
		"code":      200,
		"data":      config,
		"message":   "success",
		"timestamp": time.Now().Format(time.RFC3339),
	})
	h.logResponse(200, start)
}

func (h *StockHandler) handleNodeConfigDelete(w http.ResponseWriter, r *http.Request, start time.Time) {
	nodeID := getNodeID(r)
	if nodeID == "" {
		h.sendJSON(w, 400, map[string]interface{}{
			"code":    400,
			"message": "缺少 X-Node-ID 请求头",
		})
		return
	}

	if h.nodeStore == nil {
		h.sendJSON(w, 503, map[string]interface{}{
			"code":    503,
			"message": "节点配置存储不可用",
		})
		return
	}

	if h.nodeStore.Delete(nodeID) {
		config := h.nodeStore.Get(nodeID)
		h.sendJSON(w, 200, map[string]interface{}{
			"code":      200,
			"data":      config,
			"message":   "success",
			"timestamp": time.Now().Format(time.RFC3339),
		})
	} else {
		h.sendJSON(w, 404, map[string]interface{}{
			"code":    404,
			"message": fmt.Sprintf("节点 %s 不存在", nodeID),
		})
	}
	h.logResponse(200, start)
}

// HandleRealtime 实时行情
func (h *StockHandler) HandleRealtime(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.logRequest(r)

	code := extractPathSuffix(r.URL.Path, "/api/realtime/")
	if code == "" {
		h.sendJSON(w, 400, map[string]interface{}{
			"code":    400,
			"message": "缺少股票代码",
		})
		return
	}

	// 通达信优先 → 腾讯 HTTP 回退
	var data *RealtimeData
	var err error

	if h.tdx != nil && h.tdx.Enabled() {
		data, err = h.tdx.GetRealtime(code)
		if err != nil && h.debug {
			log.Printf("[TDX] 实时行情获取失败 [%s]: %v，回退到腾讯 HTTP", code, err)
		}
	}

	if data == nil || err != nil {
		data, err = h.fetcher.FetchRealtime(code)
	}

	if err != nil || data == nil {
		h.sendJSON(w, 503, map[string]interface{}{
			"code":    503,
			"message": fmt.Sprintf("无可用数据: %v", err),
		})
		return
	}

	now := time.Now()
	h.sendJSON(w, 200, map[string]interface{}{
		"code":      200,
		"data":      data,
		"stale":     false,
		"price_ts":  now.Format(time.RFC3339),
		"timestamp": now.Format(time.RFC3339),
	})
	h.logResponse(200, start)
}

// HandleKline K线数据
func (h *StockHandler) HandleKline(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.logRequest(r)

	code := extractPathSuffix(r.URL.Path, "/api/kline/")
	if code == "" {
		h.sendJSON(w, 400, map[string]interface{}{
			"code":    400,
			"message": "缺少股票代码",
		})
		return
	}

	days := 30
	if d := r.URL.Query().Get("days"); d != "" {
		if v, err := strconv.Atoi(d); err == nil && v > 0 {
			days = v
		}
	}

	// 通达信优先 → 腾讯 HTTP 回退
	var data *KlineResponse
	var err error

	if h.tdx != nil && h.tdx.Enabled() {
		data, err = h.tdx.GetKline(code, days)
		if err != nil && h.debug {
			log.Printf("[TDX] K线获取失败 [%s]: %v，回退到腾讯 HTTP", code, err)
		}
	}

	if data == nil || err != nil {
		data, err = h.fetcher.FetchKline(code, days)
	}

	if err != nil || data == nil || data.Count == 0 {
		h.sendJSON(w, 503, map[string]interface{}{
			"code":    503,
			"message": fmt.Sprintf("无可用数据: %v", err),
		})
		return
	}

	h.sendJSON(w, 200, map[string]interface{}{
		"code":      200,
		"data":      data,
		"stale":     false,
		"price_ts":  time.Now().Format(time.RFC3339),
		"timestamp": time.Now().Format(time.RFC3339),
	})
	h.logResponse(200, start)
}

// HandleIntraday 分时数据
func (h *StockHandler) HandleIntraday(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.logRequest(r)

	code := extractPathSuffix(r.URL.Path, "/api/intraday/")
	if code == "" {
		h.sendJSON(w, 400, map[string]interface{}{
			"code":    400,
			"message": "缺少股票代码",
		})
		return
	}

	date := r.URL.Query().Get("date")

	// 通达信优先 → 腾讯 HTTP 回退
	var data *IntradayResponse
	var err error

	if h.tdx != nil && h.tdx.Enabled() {
		data, err = h.tdx.GetIntraday(code, date)
		if err != nil && h.debug {
			log.Printf("[TDX] 分时数据获取失败 [%s]: %v，回退到腾讯 HTTP", code, err)
		}
	}

	if data == nil || err != nil {
		data, err = h.fetcher.FetchIntraday(code, date)
	}

	if err != nil || data == nil || data.Count == 0 {
		h.sendJSON(w, 503, map[string]interface{}{
			"code":    503,
			"message": fmt.Sprintf("分时数据不可用: %v", err),
		})
		return
	}

	h.sendJSON(w, 200, map[string]interface{}{
		"code":      200,
		"data":      data,
		"timestamp": time.Now().Format(time.RFC3339),
	})
	h.logResponse(200, start)
}

// ── 批量实时行情 ──────────────────────────────────────────────────────────────

// fetchBatchQuotes 批量行情取数：腾讯 HTTP 全量 + TDX 真实时覆盖（三层兜底）。
func (h *StockHandler) fetchBatchQuotes(codes []string) map[string]*QuoteRecord {
	// 腾讯 HTTP：一次取全部（含名称 / 昨收 / 开盘 / 实时价）
	ten, tenErr := h.fetcher.FetchBatchQuotes(codes)
	if tenErr != nil && h.debug {
		log.Printf("[BATCH] 腾讯批量失败: %v", tenErr)
	}
	if ten == nil {
		ten = make(map[string]*QuoteRecord)
	}

	// TDX 优先生效（真实时 / 昨收兜底）
	if h.tdx != nil && h.tdx.Enabled() {
		tq, err := h.tdx.GetBatchQuotes(codes)
		if err != nil && h.debug {
			log.Printf("[BATCH] TDX 批量失败: %v", err)
		}
		for code, q := range tq {
			if existing, ok := ten[code]; ok && existing != nil {
				if q.Price > 0 {
					existing.Price = q.Price
					existing.High = q.High
					existing.Low = q.Low
					existing.PriceSource = "" // 实时价
				} else if q.PriceSource == "last_close" {
					existing.Price = q.Price
					existing.PriceSource = "last_close"
				}
			}
		}
	}

	// 无 TDX 时，腾讯价即实时价（非 close 兜底）
	if h.tdx == nil || !h.tdx.Enabled() {
		for _, rec := range ten {
			if rec != nil {
				rec.PriceSource = ""
			}
		}
	}
	return ten
}

// HandleBatchQuotes 批量实时行情（一次取多只）
func (h *StockHandler) HandleBatchQuotes(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.logRequest(r)

	codesParam := r.URL.Query().Get("codes")
	if codesParam == "" {
		h.sendJSON(w, 400, map[string]interface{}{"code": 400, "message": "缺少 codes 参数"})
		return
	}
	codes := make([]string, 0)
	for _, c := range strings.Split(codesParam, ",") {
		c = strings.TrimSpace(c)
		if c != "" {
			codes = append(codes, c)
		}
	}
	if len(codes) == 0 {
		h.sendJSON(w, 400, map[string]interface{}{"code": 400, "message": "缺少股票代码"})
		return
	}

	result := h.fetchBatchQuotes(codes)
	if len(result) == 0 {
		h.sendJSON(w, 503, map[string]interface{}{"code": 503, "message": "无可用数据源"})
		return
	}

	// 写入共享缓存（与 /api/quote_cache 同源）
	h.quoteCache.UpsertMany(result)

	anyStale := false
	for _, rec := range result {
		if rec.Stale {
			anyStale = true
		}
	}

	now := time.Now()
	h.sendJSON(w, 200, map[string]interface{}{
		"code":      200,
		"message":   "success",
		"data":      result,
		"stale":     anyStale,
		"timestamp": now.Format(time.RFC3339),
	})
	h.logResponse(200, start)
}

// ── 前复权日 K 线 ─────────────────────────────────────────────────────────────

// HandleQfq 前复权日 K 线（供昨收兜底）
func (h *StockHandler) HandleQfq(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.logRequest(r)

	code := extractPathSuffix(r.URL.Path, "/api/qfq/")
	if code == "" {
		h.sendJSON(w, 400, map[string]interface{}{"code": 400, "message": "缺少股票代码"})
		return
	}

	days := 30
	if d := r.URL.Query().Get("days"); d != "" {
		if v, err := strconv.Atoi(d); err == nil && v > 0 {
			days = v
		}
	}

	stale := false
	resp, source, unadjusted, err := h.fetcher.FetchQfq(code, days)
	if err != nil || resp == nil || resp.Count == 0 {
		// 腾讯 HTTP 失败 → TDX 未复权日 K 兜底（对齐 Python 主链第三层：
		// 网络源失败 → 本地 TDX 前复权 → 未复权兜底并标记 unadjusted）。
		if h.tdx != nil && h.tdx.Enabled() {
			if tdxResp, tdxErr := h.tdx.GetKline(code, days); tdxErr == nil && tdxResp != nil && tdxResp.Count > 0 {
				log.Printf("[QFQ] 腾讯HTTP失败(%v)，TDX 未复权兜底 [%s]", err, code)
				resp = tdxResp
				source = "通达信TCP(未复权)"
				unadjusted = true
				err = nil
			}
		}
	}
	if err != nil || resp == nil || resp.Count == 0 {
		// 腾讯 + TDX 均不可用 → 读共享库 cn_stock_hist 前复权日 K 兜底
		// （对齐 Python 读库策略：数据可能非最新交易日，标记 stale）。
		if dbResp := h.fetchHistFromDB(code, days); dbResp != nil && dbResp.Count > 0 {
			log.Printf("[QFQ] 实时源均失败，DB cn_stock_hist 兜底 [%s]", code)
			resp = dbResp
			source = "DB历史(前复权)"
			unadjusted = false
			stale = true
			err = nil
		}
	}
	if err != nil || resp == nil || resp.Count == 0 {
		h.sendJSON(w, 503, map[string]interface{}{
			"code":    503,
			"message": fmt.Sprintf("前复权K线不可用: %v", err),
		})
		return
	}

	// 盘中补「当日根」：网络前复权源不含未收盘当日 K 线，末根停在昨天，
	// 补上后末根 open 才是真正的今开（与 Python ensure_today_open 对齐）。
	resp = h.ensureTodayOpen(resp, code)

	now := time.Now()
	h.sendJSON(w, 200, map[string]interface{}{
		"code": 200,
		"data": map[string]interface{}{
			"code":       code,
			"name":       resp.Name,
			"count":      resp.Count,
			"data":       resp.Data,
			"stale":      stale,
			"price_ts":   now.Format(time.RFC3339),
			"source":     source,
			"unadjusted": unadjusted,
		},
		"timestamp": now.Format(time.RFC3339),
	})
	h.logResponse(200, start)
}

// fetchHistFromDB 读共享 SQLite 库 cn_stock_hist 前复权日 K 兜底
// （与 Python 版读库策略对齐；数据可能非最新交易日，由调用方标记 stale）。
func (h *StockHandler) fetchHistFromDB(code string, days int) *KlineResponse {
	db := h.quoteCache.DB()
	if db == nil {
		return nil
	}
	rows, err := db.Query(
		`SELECT date, open, close, high, low, volume
		   FROM cn_stock_hist WHERE code = ? ORDER BY date DESC LIMIT ?`,
		code, days)
	if err != nil {
		if h.debug {
			log.Printf("[QFQ] cn_stock_hist 查询失败: %v", err)
		}
		return nil
	}
	defer rows.Close()

	records := make([]KlineRecord, 0, days)
	for rows.Next() {
		var rec KlineRecord
		if err := rows.Scan(&rec.Date, &rec.Open, &rec.Close, &rec.High, &rec.Low, &rec.Volume); err != nil {
			if h.debug {
				log.Printf("[QFQ] cn_stock_hist 行解析失败: %v", err)
			}
			return nil
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil
	}
	if len(records) == 0 {
		return nil
	}
	// 查询为 date DESC，反转为升序（与实时源返回一致，末根为最新）
	for i, j := 0, len(records)-1; i < j; i, j = i+1, j-1 {
		records[i], records[j] = records[j], records[i]
	}
	return &KlineResponse{
		Code:  code,
		Name:  h.fetcher.getNameCache(code),
		Count: len(records),
		Data:  records,
	}
}

// ensureTodayOpen 若 qfq 末根不是今天，则用 TDX 未复权当日日线补一根（前复权锚定最新交易日，无跳变）
func (h *StockHandler) ensureTodayOpen(resp *KlineResponse, code string) *KlineResponse {
	if resp == nil || len(resp.Data) == 0 {
		return resp
	}
	today := time.Now().Format("2006-01-02")
	if resp.Data[len(resp.Data)-1].Date >= today {
		return resp
	}
	if h.tdx != nil && h.tdx.Enabled() {
		dayResp, err := h.tdx.GetKline(code, 1)
		if err == nil && dayResp != nil && dayResp.Count > 0 {
			bar := dayResp.Data[0]
			if bar.Date == today || bar.Date == strings.ReplaceAll(today, "-", "") {
				resp.Data = append(resp.Data, KlineRecord{
					Date:   today,
					Open:   bar.Open,
					Close:  bar.Close,
					High:   bar.High,
					Low:    bar.Low,
					Volume: bar.Volume,
				})
				resp.Count = len(resp.Data)
			}
		}
	}
	return resp
}

// ── 股票名称 ─────────────────────────────────────────────────────────────────

// HandleName 股票名称查询
func (h *StockHandler) HandleName(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.logRequest(r)

	code := extractPathSuffix(r.URL.Path, "/api/name/")
	if code == "" {
		h.sendJSON(w, 400, map[string]interface{}{"code": 400, "message": "缺少股票代码"})
		return
	}

	name, err := h.fetcher.FetchName(code)
	if err != nil {
		name = "" // 解析不到时返回空串（客户端按取不到名降级），不返回 404
	}

	now := time.Now()
	h.sendJSON(w, 200, map[string]interface{}{
		"code": 200,
		"data": map[string]interface{}{
			"code": code,
			"name": name,
		},
		"timestamp": now.Format(time.RFC3339),
	})
	h.logResponse(200, start)
}

// ── 分钟 K 线 ────────────────────────────────────────────────────────────────

// HandleMinute 分钟 K 线（原始 OHLC 棒，供分时检测复用）
func (h *StockHandler) HandleMinute(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.logRequest(r)

	code := extractPathSuffix(r.URL.Path, "/api/minute/")
	if code == "" {
		h.sendJSON(w, 400, map[string]interface{}{"code": 400, "message": "缺少股票代码"})
		return
	}

	period := 7
	if p := r.URL.Query().Get("period"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			period = v
		}
	}
	minutes := 300
	if m := r.URL.Query().Get("minutes"); m != "" {
		if v, err := strconv.Atoi(m); err == nil {
			minutes = v
		}
	}

	var data *MinuteResponse
	var err error
	if h.tdx != nil && h.tdx.Enabled() {
		data, err = h.tdx.GetMinute(code, period, minutes)
	}
	if err != nil || data == nil || data.Count == 0 {
		h.sendJSON(w, 503, map[string]interface{}{
			"code":    503,
			"message": fmt.Sprintf("无可用分钟数据源: %v", err),
		})
		return
	}

	now := time.Now()
	h.sendJSON(w, 200, map[string]interface{}{
		"code": 200,
		"data": map[string]interface{}{
			"code":  code,
			"count": data.Count,
			"data":  data.Data,
		},
		"timestamp": now.Format(time.RFC3339),
	})
	h.logResponse(200, start)
}

// ── 实时价格缓存 ─────────────────────────────────────────────────────────────

// HandleQuoteCache 实时价格缓存（进程内共享，对标 Python cn_quote_cache）
func (h *StockHandler) HandleQuoteCache(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.logRequest(r)

	codesParam := r.URL.Query().Get("codes")
	now := time.Now()

	var data map[string]*QuoteCacheEntry
	if codesParam != "" {
		data = make(map[string]*QuoteCacheEntry)
		for _, c := range strings.Split(codesParam, ",") {
			c = strings.TrimSpace(c)
			if c == "" {
				continue
			}
			if rec := h.quoteCache.Get(c); rec != nil {
				data[rec.Code] = rec
			}
		}
	} else {
		data = h.quoteCache.GetAll()
	}

	h.sendJSON(w, 200, map[string]interface{}{
		"code":      200,
		"message":   "success",
		"data":      data,
		"timestamp": now.Format(time.RFC3339),
	})
	h.logResponse(200, start)
}

// logResponse 记录响应
func (h *StockHandler) logResponse(code int, start time.Time) {
	if !h.debug {
		return
	}
	elapsed := getElapsedMS(start)
	log.Printf("══════════════════════════════════════════════════════════")
	log.Printf("[RESPONSE] HTTP %d", code)
	log.Printf("  处理时间: %.3f ms", elapsed)
	log.Printf("══════════════════════════════════════════════════════════")
}

// extractPathSuffix 提取路径后缀
func extractPathSuffix(path, prefix string) string {
	s := strings.TrimPrefix(path, prefix)
	s = strings.TrimSuffix(s, "/")
	return s
}
