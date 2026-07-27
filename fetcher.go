// go-stock-server/fetcher.go - 股票数据获取（腾讯 HTTP API）
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
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

// parseKline 解析 K 线数据（标准 JSON 解码，覆盖 qfqday / day 两种字段）
func (f *StockFetcher) parseKline(content, code string, days int) (*KlineResponse, error) {
	key := fmt.Sprintf("%s%s", marketPrefix(code), code)

	var root struct {
		Code int `json:"code"`
		Data map[string]struct {
			QfqDay [][]interface{} `json:"qfqday"`
			Day    [][]interface{} `json:"day"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(content), &root); err != nil {
		return nil, fmt.Errorf("K线 JSON 解析失败: %w", err)
	}

	node, ok := root.Data[key]
	if !ok {
		node, ok = root.Data[code]
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
		parts = append(parts, marketPrefix(c)+c)
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
		marker := fmt.Sprintf("v_%s%s=", marketPrefix(c), c)
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
		result[c] = &QuoteRecord{
			Code:      c,
			Name:      name,
			Price:     parseFloatSafe(fields[3]),
			PrevClose: parseFloatSafe(fields[4]),
			Open:      parseFloatSafe(fields[5]),
			High:      parseFloatSafe(fields[33]),
			Low:       parseFloatSafe(fields[34]),
			Volume:    parseFloatSafe(fields[6]) * 100,
			Amount:    parseFloatSafe(fields[37]) * 10000,
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

// FetchQfq 前复权日 K 线（腾讯 HTTP，qfq 参数）。
// 返回 K线响应、数据源标识、是否未复权兜底、错误。
func (f *StockFetcher) FetchQfq(code string, days int) (*KlineResponse, string, bool, error) {
	resp, err := f.FetchKline(code, days)
	if err != nil {
		return nil, "", false, err
	}
	if resp == nil || resp.Count == 0 {
		return nil, "", false, fmt.Errorf("前复权K线无数据 [%s]", code)
	}
	return resp, "腾讯A股", false, nil
}
