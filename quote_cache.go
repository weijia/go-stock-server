// go-stock-server/quote_cache.go - 实时价格共享缓存（进程内，对标 Python cn_quote_cache）
package main

import (
	"sync"
	"time"
)

// QuoteCacheEntry 缓存中的单条实时价记录
type QuoteCacheEntry struct {
	Code      string  `json:"code"`
	Name      string  `json:"name"`
	Price     float64 `json:"price"`
	Open      float64 `json:"open"`
	PrevClose float64 `json:"prev_close"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	PriceTS   int64   `json:"price_ts"` // 数据取到时刻（unix 秒）
	UpdatedAt int64   `json:"updated_at"`
}

// QuoteCache 实时价格缓存（HTTP / MQTT 共享同一实例）
type QuoteCache struct {
	mu sync.RWMutex
	m  map[string]*QuoteCacheEntry
}

// NewQuoteCache 创建缓存
func NewQuoteCache() *QuoteCache {
	return &QuoteCache{m: make(map[string]*QuoteCacheEntry)}
}

// normalizeCode 将带前缀代码规范为 6 位
func normalizeCode(code string) string {
	if len(code) >= 6 {
		return code[len(code)-6:]
	}
	return code
}

// UpsertMany 批量写入（来自批量行情结果）
func (c *QuoteCache) UpsertMany(records map[string]*QuoteRecord) {
	now := time.Now().Unix()
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, r := range records {
		if r == nil || r.Price <= 0 {
			continue
		}
		key := normalizeCode(k)
		c.m[key] = &QuoteCacheEntry{
			Code:      key,
			Name:      r.Name,
			Price:     r.Price,
			Open:      r.Open,
			PrevClose: r.PrevClose,
			High:      r.High,
			Low:       r.Low,
			PriceTS:   r.PriceTS,
			UpdatedAt: now,
		}
	}
}

// Get 获取单条（6 位或带前缀代码均可）
func (c *QuoteCache) Get(code string) *QuoteCacheEntry {
	key := normalizeCode(code)
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.m[key]
}

// GetAll 返回全部缓存副本
func (c *QuoteCache) GetAll() map[string]*QuoteCacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]*QuoteCacheEntry, len(c.m))
	for k, v := range c.m {
		cp := *v
		out[k] = &cp
	}
	return out
}
