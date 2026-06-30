// go-stock-server/tdx_source.go — 通达信 TCP 数据源接入
package main

import (
	"fmt"
	"log"
	"time"

	tdx "github.com/weijia/go-tdx"
)

// TdxDataSource 通达信 TCP 行情数据源
type TdxDataSource struct {
	provider *tdx.Provider
	enabled  bool
	debug    bool
}

// NewTdxDataSource 创建通达信数据源
// autoConnect: true=自动测速连接最优服务器
func NewTdxDataSource(autoConnect bool, debug bool) *TdxDataSource {
	ds := &TdxDataSource{
		provider: tdx.NewProvider(),
		debug:    debug,
	}
	ds.provider.SetDebug(debug)

	if autoConnect {
		if debug {
			log.Println("[TDX] Server: 正在测速选择最优服务器...")
		}
		err := ds.provider.ConnectBest()
		if err != nil {
			log.Printf("[TDX] ⚠️ 自动连接失败: %v（回退到腾讯 HTTP）", err)
			return ds
		}
		ds.enabled = true
		if debug {
			log.Println("[TDX] ✅ 通达信 TCP 数据源已就绪")
		}
	}
	return ds
}

// Enabled 是否已连接
func (ds *TdxDataSource) Enabled() bool {
	return ds.enabled && ds.provider.IsConnected()
}

// Close 关闭连接
func (ds *TdxDataSource) Close() {
	if ds.provider != nil {
		ds.provider.Close()
	}
	ds.enabled = false
}

// ── 接口适配 (返回格式与现有 fetcher 兼容) ─────────────────────────────────

// GetKline 通过通达信获取K线数据
func (ds *TdxDataSource) GetKline(code string, days int) (*KlineResponse, error) {
	if !ds.Enabled() {
		return nil, fmt.Errorf("TDX 数据源未连接")
	}

	market := tdx.ResolveMarket(code)
	category := tdx.CategoryKLineDay

	bars, err := ds.provider.GetKline(uint16(category), uint16(market), code, 0, uint16(days))
	if err != nil {
		return nil, fmt.Errorf("TDX K线获取失败 [%s]: %w", code, err)
	}

	var records []KlineRecord
	for _, bar := range bars {
		records = append(records, KlineRecord{
			Date:   formatDate(bar.Date),
			Open:   bar.Open,
			Close:  bar.Close,
			High:   bar.High,
			Low:    bar.Low,
			Volume: bar.Volume,
		})
	}

	return &KlineResponse{
		Code:  code,
		Name:  "",
		Count: len(records),
		Data:  records,
	}, nil
}

// GetRealtime 通过通达信获取实时行情
func (ds *TdxDataSource) GetRealtime(code string) (*RealtimeData, error) {
	if !ds.Enabled() {
		return nil, fmt.Errorf("TDX 数据源未连接")
	}

	market := tdx.ResolveMarket(code)
	bars, err := ds.provider.GetKline(uint16(tdx.CategoryKLineDay), uint16(market), code, 0, 2)
	if err != nil {
		return nil, fmt.Errorf("TDX 实时行情获取失败 [%s]: %w", code, err)
	}

	if len(bars) == 0 {
		return nil, fmt.Errorf("TDX 返回空数据 [%s]", code)
	}

	latest := bars[len(bars)-1]
	var prev tdx.KlineBar
	if len(bars) >= 2 {
		prev = bars[len(bars)-2]
	}

	changePct := 0.0
	changeAmt := 0.0
	if prev.Close > 0 {
		changeAmt = latest.Close - prev.Close
		changePct = changeAmt / prev.Close * 100
	}

	return &RealtimeData{
		Name:      "",
		Code:      code,
		Price:     latest.Close,
		LastClose: prev.Close,
		Open:      latest.Open,
		High:      latest.High,
		Low:       latest.Low,
		Volume:    latest.Volume,
		Amount:    latest.Amount,
		ChangeAmt: changeAmt,
		ChangePct: changePct,
	}, nil
}

// GetIntraday 通过通达信获取分时数据
// 使用 1 分钟 K线模拟分时数据
func (ds *TdxDataSource) GetIntraday(code string, date string) (*IntradayResponse, error) {
	if !ds.Enabled() {
		return nil, fmt.Errorf("TDX 数据源未连接")
	}

	market := tdx.ResolveMarket(code)
	// 一分钟K线，获取足够多的数量覆盖全天 (约240根)
	bars, err := ds.provider.GetKline(uint16(tdx.CategoryKLine1Min), uint16(market), code, 0, 241)
	if err != nil {
		return nil, fmt.Errorf("TDX 分时数据获取失败 [%s]: %w", code, err)
	}

	if len(bars) == 0 {
		return nil, fmt.Errorf("TDX 返回空分时数据 [%s]", code)
	}

	// 过滤当天的数据
	var intradayData []IntradayRecord
	preClose := 0.0

	for i, bar := range bars {
		// 日期格式过滤 (如果当天没有 1min K线，则用最近一天)
		if i == 0 && len(bars) > 1 {
			preClose = bar.Close
		}

		timeStr := ""
		if len(bar.Date) >= 12 {
			// YYYYMMDDHHMM 格式
			timeStr = bar.Date[8:10] + ":" + bar.Date[10:12]
		} else if len(bar.Date) == 8 {
			// YYYYMMDD 只有日期
			timeStr = "15:00"
		}

		totalVolume := 0.0
		totalAmount := 0.0
		for j := 0; j <= i; j++ {
			totalVolume += bars[j].Volume
			totalAmount += bars[j].Amount
		}
		avgPrice := 0.0
		if totalVolume > 0 {
			avgPrice = totalAmount / totalVolume
		}

		intradayData = append(intradayData, IntradayRecord{
			Time:     timeStr,
			Price:    bar.Close,
			Volume:   bar.Volume,
			AvgPrice: roundTo(avgPrice, 2),
		})
	}

	dataDate := time.Now().Format("2006-01-02")
	if date != "" {
		if len(date) == 8 {
			dataDate = fmt.Sprintf("%s-%s-%s", date[0:4], date[4:6], date[6:8])
		}
	}

	return &IntradayResponse{
		StockCode: code,
		Name:      "",
		Date:      dataDate,
		Count:     len(intradayData),
		PreClose:  preClose,
		Data:      intradayData,
	}, nil
}

// ── 工具函数 ──────────────────────────────────────────────────────────────────

// formatDate 将 YYYYMMDD 或 YYYYMMDDHHMM 格式化为可显示格式
func formatDate(dateStr string) string {
	if len(dateStr) >= 8 {
		return dateStr[0:4] + "-" + dateStr[4:6] + "-" + dateStr[6:8]
	}
	return dateStr
}


