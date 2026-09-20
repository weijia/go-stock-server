package main

import (
	"testing"
	"time"
)

func TestFetchKlineTmp(t *testing.T) {
	f := NewStockFetcher(true)
	resp, src, stale, err := f.FetchKline("000001", 3)
	if err != nil {
		t.Fatalf("FetchKline 000001 ERR: %v", err)
	}
	t.Logf("000001 OK source=%s stale=%v count=%d last=%v", src, stale, resp.Count, resp.Data[len(resp.Data)-1].Date)

	// 510300 沪深300ETF：验证 ETF 也能出 qfq（此前单源腾讯被拦必 503）
	respE, srcE, staleE, errE := f.FetchKline("510300", 3)
	if errE != nil {
		t.Fatalf("FetchKline 510300(ETF) ERR: %v", errE)
	}
	t.Logf("510300 OK source=%s stale=%v count=%d last=%v", srcE, staleE, respE.Count, respE.Data[len(respE.Data)-1].Date)

	// 无效代码：所有源都应失败，仅断言 err != nil（具体来源错误随环境而异）。
	_, _, _, err2 := f.FetchKline("000000", 3)
	if err2 == nil {
		t.Fatalf("000000 应失败但返回了数据")
	}
	t.Logf("000000 ERR: %v", err2)
}

func TestSinaRaw(t *testing.T) {
	f := NewStockFetcher(true)
	url := "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol=sz000001&scale=240&ma=no&datalen=5"
	body, err := f.httpGetWithHeaders(url, map[string]string{"Referer": "https://finance.sina.com.cn/"}, 8*time.Second)
	t.Logf("err=%v body=%q", err, body)
}
