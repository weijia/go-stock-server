// go-stock-server/tdx_source.go — 通达信 TCP 数据源接入
package core

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

// ── 批量 / 分钟 ──────────────────────────────────────────────────────────────

// GetBatchQuotes 通过通达信批量获取实时行情（真实时五档命令）
// 价格分层兜底：price>0 → 实时价；price=0 但昨收>0 → 昨收；否则跳过（交腾讯兜底）。
func (ds *TdxDataSource) GetBatchQuotes(codes []string) (map[string]*QuoteRecord, error) {
	if !ds.Enabled() {
		return nil, fmt.Errorf("TDX 数据源未连接")
	}
	quotes, err := ds.provider.GetQuotesBatch(codes)
	if err != nil {
		return nil, fmt.Errorf("TDX 批量行情失败: %w", err)
	}

	result := make(map[string]*QuoteRecord)
	now := time.Now().Unix()
	for _, q := range quotes {
		rec := &QuoteRecord{
			Code:      q.Code,
			Name:      q.Name,
			Open:      q.Open,
			High:      q.High,
			Low:       q.Low,
			PrevClose: q.LastClose,
			Volume:    q.Volume,
			Amount:    q.Amount,
			PriceTS:   now,
			Stale:     false,
		}
		switch {
		case q.Price > 0:
			rec.Price = q.Price
			rec.PriceSource = "" // 实时价
		case q.LastClose > 0:
			rec.Price = q.LastClose
			rec.PriceSource = "last_close"
		default:
			continue // 无价无昨收，交给腾讯兜底
		}
		result[q.Code] = rec
	}
	return result, nil
}

// GetMinute 通过通达信获取分钟 K 线（原始 OHLC 棒）
func (ds *TdxDataSource) GetMinute(code string, period int, minutes int) (*MinuteResponse, error) {
	if !ds.Enabled() {
		return nil, fmt.Errorf("TDX 数据源未连接")
	}
	market := tdx.ResolveMarket(code)
	cat, ok := minuteCategory(period)
	if !ok {
		cat = tdx.CategoryKLine1Min
	}
	if minutes <= 0 {
		minutes = 300
	}
	bars, err := ds.provider.GetKline(uint16(cat), uint16(market), code, 0, uint16(minutes))
	if err != nil {
		return nil, fmt.Errorf("TDX 分钟K线失败 [%s]: %w", code, err)
	}
	if len(bars) == 0 {
		return nil, fmt.Errorf("TDX 分钟K线为空 [%s]", code)
	}
	records := make([]KlineRecord, 0, len(bars))
	for _, b := range bars {
		records = append(records, KlineRecord{
			Date:   b.Date,
			Open:   b.Open,
			Close:  b.Close,
			High:   b.High,
			Low:    b.Low,
			Volume: b.Volume,
		})
	}
	return &MinuteResponse{Code: code, Count: len(records), Data: records}, nil
}

// minuteCategory 将 mootdx 周期代码映射到 TDX category 常量（pytdx 与 go-tdx 对齐）。
func minuteCategory(period int) (uint16, bool) {
	switch period {
	case 0:
		return tdx.CategoryKLine5Min, true
	case 1:
		return tdx.CategoryKLine15Min, true
	case 2:
		return tdx.CategoryKLine30Min, true
	case 3:
		return tdx.CategoryKLine1Hour, true
	case 4:
		return tdx.CategoryKLineDay, true
	case 7:
		return tdx.CategoryKLine1Min, true
	default:
		return tdx.CategoryKLine1Min, false
	}
}


