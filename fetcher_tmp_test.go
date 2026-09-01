package main

import (
	"strings"
	"testing"
)

func TestFetchKlineTmp(t *testing.T) {
	f := NewStockFetcher(false)
	resp, err := f.FetchKline("sz000001", 3)
	if err != nil {
		t.Fatalf("FetchKline sz000001 ERR: %v", err)
	}
	t.Logf("sz000001 OK count=%d last=%v", resp.Count, resp.Data[len(resp.Data)-1].Date)

	resp2, err2 := f.FetchKline("text", 3)
	if err2 == nil {
		t.Fatalf("text should fail, got %+v", resp2)
	}
	t.Logf("text ERR: %v", err2)
	if !strings.Contains(err2.Error(), "K线数据未找到") {
		t.Fatalf("unexpected msg: %v", err2)
	}
}
