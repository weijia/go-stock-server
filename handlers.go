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
	debug      bool
}

// NewStockHandler 创建处理器
func NewStockHandler(fetcher *StockFetcher, tdx *TdxDataSource, nodeStore *NodeConfigStore, debug bool) *StockHandler {
	return &StockHandler{
		fetcher:   fetcher,
		tdx:       tdx,
		nodeStore: nodeStore,
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

	h.sendJSON(w, 200, map[string]interface{}{
		"code":      200,
		"data":      data,
		"timestamp": time.Now().Format(time.RFC3339),
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
