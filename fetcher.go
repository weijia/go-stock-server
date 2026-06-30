// go-stock-server/fetcher.go - 股票数据获取（腾讯 HTTP API）
package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// StockFetcher 股票数据获取器
type StockFetcher struct {
	client    *http.Client
	debug     bool
	nameCache map[string]string
	mu        sync.RWMutex
}

// NewStockFetcher 创建数据获取器
func NewStockFetcher(debug bool) *StockFetcher {
	return &StockFetcher{
		client:    &http.Client{Timeout: 15 * time.Second},
		debug:     debug,
		nameCache: make(map[string]string),
	}
}

// marketPrefix 根据股票代码判断市场前缀
func marketPrefix(code string) string {
	code = strings.TrimSpace(code)
	if strings.HasPrefix(code, "6") || strings.HasPrefix(code, "5") ||
		strings.HasPrefix(code, "9") {
		return "sh"
	}
	return "sz"
}

// FetchRealtime 获取实时行情
func (f *StockFetcher) FetchRealtime(code string) (*RealtimeData, error) {
	prefix := marketPrefix(code)
	url := fmt.Sprintf("http://qt.gtimg.cn/q=%s%s", prefix, code)

	body, err := f.httpGet(url)
	if err != nil {
		return nil, fmt.Errorf("请求实时行情失败: %w", err)
	}

	return f.parseRealtime(body, code)
}

// FetchKline 获取K线数据
func (f *StockFetcher) FetchKline(code string, days int) (*KlineResponse, error) {
	prefix := marketPrefix(code)
	url := fmt.Sprintf("http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s%s,day,,,%d,qfq", prefix, code, days)

	body, err := f.httpGet(url)
	if err != nil {
		return nil, fmt.Errorf("请求K线数据失败: %w", err)
	}

	return f.parseKline(body, code, days)
}

// FetchIntraday 获取分时数据
func (f *StockFetcher) FetchIntraday(code string, date string) (*IntradayResponse, error) {
	// 使用腾讯分时 API
	prefix := marketPrefix(code)
	url := fmt.Sprintf("http://ifzq.gtimg.cn/appstock/app/minute/query?_var=min_data&code=%s%s", prefix, code)

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
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

// parseRealtime 解析实时行情
func (f *StockFetcher) parseRealtime(content, code string) (*RealtimeData, error) {
	key := fmt.Sprintf("v_%s%s=", marketPrefix(code), code)

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

	return &RealtimeData{
		Name:      name,
		Code:      code,
		Price:     parseFloatSafe(parts[3]),
		LastClose: parseFloatSafe(parts[4]),
		Open:      parseFloatSafe(parts[5]),
		High:      parseFloatSafe(parts[33]),
		Low:       parseFloatSafe(parts[34]),
		Volume:    parseFloatSafe(parts[6]) * 100,        // 手转股
		Amount:    parseFloatSafe(parts[37]) * 10000,     // 万元转元
		ChangeAmt: parseFloatSafe(parts[31]),
		ChangePct: parseFloatSafe(parts[32]),
	}, nil
}

// parseKline 解析 K 线数据
func (f *StockFetcher) parseKline(content, code string, days int) (*KlineResponse, error) {
	// 腾讯 K 线 API 返回 JSON
	rawData := content

	// 简单 JSON 解析（避免引入第三方 JSON 库）
	type TKlineData struct {
		Code int `json:"code"`
		Data map[string]struct {
			QfqDay [][]interface{} `json:"qfqday"`
			Day    [][]interface{} `json:"day"`
		} `json:"data"`
	}

	// 这里需要引入 json 库
	// 使用 encoding/json
	// 但为了简化，这里使用标准库的 encoding/json
	_ = rawData

	key := fmt.Sprintf("%s%s", marketPrefix(code), code)

	// 手动解析 JSON（简化版）
	var records []KlineRecord
	klineRaw := extractJSONArray(rawData, key, "qfqday")
	if len(klineRaw) == 0 {
		klineRaw = extractJSONArray(rawData, key, "day")
	}
	if len(klineRaw) == 0 {
		// fallback: try without market prefix
		klineRaw = extractJSONArray(rawData, code, "qfqday")
	}

	for _, item := range klineRaw {
		switch v := item.(type) {
		case []interface{}:
			if len(v) >= 6 {
				date, _ := v[0].(string)
				open := toFloat(v[1])
				close := toFloat(v[2])
				high := toFloat(v[3])
				low := toFloat(v[4])
				volume := toFloat(v[5])
				records = append(records, KlineRecord{
					Date:   date,
					Open:   open,
					Close:  close,
					High:   high,
					Low:    low,
					Volume: volume,
				})
			}
		}
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
	key := fmt.Sprintf("%s%s", marketPrefix(code), code)

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
