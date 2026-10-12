// go-stock-server/valuation.go - 估值历史查询接口（对标 Python handle_valuation）
//
// 与 stock/instock/server/valuation.py 行为对齐：
//   - 从共享 SQLite 库的 cn_stock_spot 表读取估值历史（pe/pe9/pbnewmrq/dv_yield
//     及多周期分位等 33 个字段），按 days 截取最近 N 条，日期降序；
//   - 响应格式与 price_monitor 的 HttpPriceSource.fetch_valuation 契约完全一致：
//     {"code":200,"data":{"code","data":[...],"count","source":"db"},"timestamp"};
//   - 对最新一条附加 40 个多周期窗口极值（*_min/*_max），口径与 Python
//     fetch_valuation_window_stats 一致（滚动窗口 min_periods=2，忽略 NULL）；
//   - ETF/基金（1/5 开头）估值字段本身为 NULL，直接返回库数据，不回源。
package core

import (
	"database/sql"
	"log"
	"net/http"
	"strconv"
	"time"
)

// valuationHistoryCols fetch_valuation_history 的 SELECT 列（顺序与 Python 版一致）
const valuationHistorySQL = `SELECT date, pe, pe9, pbnewmrq, dv_yield, price_pct, dv_pct,
	close_hfq, pe_pct_2y, pe_pct_3y, pe_pct_5y, pe_pct_10y,
	pe9_pct_2y, pe9_pct_3y, pe9_pct_5y, pe9_pct_10y,
	pb_pct_2y, pb_pct_3y, pb_pct_5y, pb_pct_10y,
	vol_pct_1y, vol_pct_6m, vol_pct_1m, vol_pct_1w,
	price_pct_hfq_2y, price_pct_hfq_3y, price_pct_hfq_5y, price_pct_hfq_10y,
	close_qfq, price_pct_qfq_2y, price_pct_qfq_3y, price_pct_qfq_5y, price_pct_qfq_10y
	FROM cn_stock_spot WHERE code=? ORDER BY date DESC LIMIT ?`

// valuationRecKeys 33 个字段名（date + 32 个数值列，顺序与 SELECT/映射对齐）
var valuationRecKeys = []string{
	"date", "pe", "pe9", "pbnewmrq", "dv_yield", "price_pct", "dv_pct", "close_hfq",
	"pe_pct_2y", "pe_pct_3y", "pe_pct_5y", "pe_pct_10y",
	"pe9_pct_2y", "pe9_pct_3y", "pe9_pct_5y", "pe9_pct_10y",
	"pb_pct_2y", "pb_pct_3y", "pb_pct_5y", "pb_pct_10y",
	"vol_pct_1y", "vol_pct_6m", "vol_pct_1m", "vol_pct_1w",
	"price_pct_hfq_2y", "price_pct_hfq_3y", "price_pct_hfq_5y", "price_pct_hfq_10y",
	"close_qfq", "price_pct_qfq_2y", "price_pct_qfq_3y", "price_pct_qfq_5y", "price_pct_qfq_10y",
}

// _PCT_WIN 滚动分位窗口（与 Python _PCT_WIN 一致，按交易日近似）
var pctWin = map[string]int{
	"2y": 486, "3y": 729, "5y": 1215, "10y": 2430,
	"1y": 243, "6m": 121, "1m": 21, "1w": 5,
}

// HandleValuation 估值历史查询：GET /api/valuation/<code>?days=N
func (h *StockHandler) HandleValuation(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.logRequest(r)

	code := extractPathSuffix(r.URL.Path, "/api/valuation/")
	if code == "" {
		h.sendJSON(w, 400, map[string]interface{}{"code": 400, "message": "缺少股票代码"})
		return
	}

	days := 250
	if d := r.URL.Query().Get("days"); d != "" {
		if v, err := strconv.Atoi(d); err == nil && v > 0 {
			days = v
		}
	}

	db := h.quoteCache.DB()
	records := fetchValuationHistory(db, code, days)
	if len(records) > 0 {
		if stats := fetchValuationWindowStats(db, code); len(stats) > 0 {
			// 与 Python handle_valuation 一致：窗口极值附加到日期最大（最新）的一条
			latest := records[0] // ORDER BY date DESC，首条即最新
			for i := 1; i < len(records); i++ {
				if records[i]["date"].(string) > latest["date"].(string) {
					latest = records[i]
				}
			}
			for k, v := range stats {
				latest[k] = v
			}
		}
	}

	now := time.Now()
	h.sendJSON(w, 200, map[string]interface{}{
		"code": 200,
		"data": map[string]interface{}{
			"code":   code,
			"data":   records,
			"count":  len(records),
			"source": "db",
		},
		"timestamp": now.Format(time.RFC3339),
	})
	h.logResponse(200, start)
}

// fetchValuationHistory 从 cn_stock_spot 读最近 days 条估值（日期降序）。
// db 为 nil（SQLite 未启用）或查询失败时返回空列表。
func fetchValuationHistory(db *sql.DB, code string, days int) []map[string]interface{} {
	if db == nil {
		return []map[string]interface{}{}
	}
	code = normalizeCode(code)
	rows, err := db.Query(valuationHistorySQL, code, days)
	if err != nil {
		log.Printf("[估值] 查询失败 %s: %v", code, err)
		return []map[string]interface{}{}
	}
	defer rows.Close()

	recs := make([]map[string]interface{}, 0)
	for rows.Next() {
		var date string
		var vals [32]sql.NullFloat64
		dest := make([]interface{}, 0, 33)
		dest = append(dest, &date)
		for i := 0; i < 32; i++ {
			dest = append(dest, &vals[i])
		}
		if err := rows.Scan(dest...); err != nil {
			continue
		}
		rec := make(map[string]interface{}, 33)
		rec["date"] = date
		for i, k := range valuationRecKeys[1:] {
			if vals[i].Valid {
				rec[k] = vals[i].Float64
			} else {
				rec[k] = nil
			}
		}
		recs = append(recs, rec)
	}
	return recs
}

// fetchValuationWindowStats 最新一日各多周期分位底层指标的窗口 min/max
// （口径与 Python fetch_valuation_window_stats 一致：滚动窗口、min_periods=2、忽略 NULL）。
func fetchValuationWindowStats(db *sql.DB, code string) map[string]interface{} {
	if db == nil {
		return nil
	}
	code = normalizeCode(code)
	rows, err := db.Query(`SELECT date, pe9, pbnewmrq, volume, close_hfq, close_qfq
		FROM cn_stock_spot WHERE code=? ORDER BY date ASC`, code)
	if err != nil {
		log.Printf("[估值] 窗口极值查询失败 %s: %v", code, err)
		return nil
	}
	defer rows.Close()

	type metricCol struct{ idx int }
	series := make([][]*float64, 6) // 0:pe9 1:pbnewmrq 2:volume 3:close_hfq 4:close_qfq
	for rows.Next() {
		var date string
		var pe9, pb, vol, hfq, qfq sql.NullFloat64
		if err := rows.Scan(&date, &pe9, &pb, &vol, &hfq, &qfq); err != nil {
			continue
		}
		series[0] = append(series[0], nullToPtr(pe9))
		series[1] = append(series[1], nullToPtr(pb))
		series[2] = append(series[2], nullToPtr(vol))
		series[3] = append(series[3], nullToPtr(hfq))
		series[4] = append(series[4], nullToPtr(qfq))
	}
	if len(series[0]) == 0 {
		return nil
	}

	out := make(map[string]interface{})
	// 分位底层指标：{pct列前缀, 序列下标}
	type statDef struct {
		prefix string
		metric int
	}
	defs := []statDef{}
	for _, w := range []string{"2y", "3y", "5y", "10y"} {
		defs = append(defs,
			statDef{"pe9_pct_" + w, 0},
			statDef{"pb_pct_" + w, 1},
			statDef{"price_pct_hfq_" + w, 3},
			statDef{"price_pct_qfq_" + w, 4},
		)
	}
	for _, w := range []string{"1y", "6m", "1m", "1w"} {
		defs = append(defs, statDef{"vol_pct_" + w, 2})
	}
	for _, d := range defs {
		mn, mx := windowMinMax(series[d.metric], pctWin[prefixWin(d.prefix)])
		out[d.prefix+"_min"] = mn
		out[d.prefix+"_max"] = mx
	}
	return out
}

func prefixWin(prefix string) string {
	// "pe9_pct_2y" -> "2y", "vol_pct_6m" -> "6m"
	s := prefix
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '_' {
			return s[i+1:]
		}
	}
	return s
}

// nullToPtr 把 NullFloat64 转为 *float64（invalid → nil，等价 Python None）
func nullToPtr(v sql.NullFloat64) *float64 {
	if !v.Valid {
		return nil
	}
	return &v.Float64
}

// windowMinMax 取序列末尾窗口内（min_periods=2，忽略 NULL）的 min/max。
// 等价 Python rolling(win, min_periods=2).min()/.max() 的 iloc[-1]。
func windowMinMax(vals []*float64, win int) (*float64, *float64) {
	if len(vals) == 0 || win <= 0 {
		return nil, nil
	}
	start := len(vals) - win
	if start < 0 {
		start = 0
	}
	var mn, mx float64
	cnt := 0
	first := true
	for i := start; i < len(vals); i++ {
		v := vals[i]
		if v == nil {
			continue
		}
		if first {
			mn, mx = *v, *v
			first = false
		} else {
			if *v < mn {
				mn = *v
			}
			if *v > mx {
				mx = *v
			}
		}
		cnt++
	}
	if cnt < 2 {
		return nil, nil
	}
	return &mn, &mx
}
