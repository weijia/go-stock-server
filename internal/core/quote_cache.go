// go-stock-server/quote_cache.go - 实时价格共享缓存（对标 Python cn_quote_cache）
//
// 与 stock/instock/server/quote_cache.py 行为对齐：
//   - 内存 map 提供快速读写（HTTP / MQTT 共享同一实例）；
//   - 每次批量写入同步 upsert 到 SQLite 表 cn_quote_cache（表结构与 Python 版完全一致），
//     实现多进程共享 + 重启后从 DB 预热内存缓存；
//   - 数据库打开/建表/写库失败时自动降级为纯内存模式，不影响服务可用性。
package core

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// QuoteCacheEntry 缓存中的单条实时价记录（字段与 Python cn_quote_cache 表一一对应）
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

// QuoteCache 实时价格缓存（内存为主 + SQLite 落库）
type QuoteCache struct {
	mu sync.RWMutex
	m  map[string]*QuoteCacheEntry
	db *sql.DB
}

// quoteRow upsert 到 DB 的单行数据
type quoteRow struct {
	code      string
	name      string
	price     float64
	open      float64
	prevClose float64
	high      *float64 // NULL 表示未提供（与 Python 版 None 一致）
	low       *float64
	priceTS   int64
	updatedAt int64
}

// NewQuoteCache 创建缓存并打开 SQLite 持久化。
// dbPath 为空或打开失败时降级为纯内存模式（不影响服务可用性）。
func NewQuoteCache(dbPath string) *QuoteCache {
	c := &QuoteCache{m: make(map[string]*QuoteCacheEntry)}
	if dbPath == "" {
		log.Println("[QuoteCache] 未配置 SQLite 路径，使用纯内存模式")
		return c
	}
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		abs = dbPath
	}
	if dir := filepath.Dir(abs); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[QuoteCache] 创建数据库目录失败: %v（降级纯内存）", err)
			return c
		}
	}
	// 与 Python 端 instock.lib.database 保持一致：WAL + busy_timeout(30s)
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)", filepath.ToSlash(abs))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		log.Printf("[QuoteCache] 打开数据库失败: %v（降级纯内存）", err)
		return c
	}
	db.SetMaxOpenConns(1) // SQLite 单写者，串行化连接避免锁冲突
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		log.Printf("[QuoteCache] 数据库连接失败: %v（降级纯内存）", err)
		db.Close()
		return c
	}
	c.db = db
	if err := c.ensureTable(); err != nil {
		log.Printf("[QuoteCache] 建表失败: %v（降级纯内存）", err)
		db.Close()
		c.db = nil
		return c
	}
	// 启动预热：从 DB 恢复最近一次有效价
	if n, err := c.loadAllQuotes(); err == nil {
		log.Printf("[QuoteCache] SQLite 持久化已启用: %s（预热 %d 条缓存）", abs, n)
	} else {
		log.Printf("[QuoteCache] 预热失败: %v（继续以空缓存运行）", err)
	}
	return c
}

// Close 关闭数据库连接
func (c *QuoteCache) Close() {
	if c.db != nil {
		c.db.Close()
	}
}

// DB 返回底层 SQLite 连接（未启用持久化时为 nil）。
// 供 /api/valuation 等需要读 stock 数据表的 handler 复用同一连接。
func (c *QuoteCache) DB() *sql.DB {
	return c.db
}

// normalizeCode 将带前缀代码规范为 6 位
func normalizeCode(code string) string {
	if len(code) >= 6 {
		return code[len(code)-6:]
	}
	return code
}

// ensureTable 确保 cn_quote_cache 表存在且 code 列为主键（结构与 Python 版 quote_cache.py 完全一致）。
// 旧版本遗留的表可能没有主键（code 无 PRIMARY KEY/UNIQUE），
// 会触发 SQLite 的 "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"，
// 这里通过探测 upsert 语句能否 Prepare 来发现并自动迁移重建（保留已有数据）。
func (c *QuoteCache) ensureTable() error {
	ddl := `CREATE TABLE IF NOT EXISTS cn_quote_cache (
    code        VARCHAR(16) PRIMARY KEY,
    name        VARCHAR(64),
    price       FLOAT,
    open        FLOAT,
    prev_close  FLOAT,
    high        FLOAT,
    low         FLOAT,
    price_ts    BIGINT,
    updated_at  BIGINT
)`
	if _, err := c.db.Exec(ddl); err != nil {
		return err
	}
	// 探测：表结构不合法（code 无主键/唯一约束）时，Prepare 直接报错
	probe, err := c.db.Prepare(`INSERT INTO cn_quote_cache
	(code,name,price,open,prev_close,high,low,price_ts,updated_at)
	VALUES (?,?,?,?,?,?,?,?,?)
	ON CONFLICT(code) DO UPDATE SET name=excluded.name`)
	if err != nil {
		return c.migrateTable()
	}
	probe.Close()
	return nil
}

// migrateTable 重建 cn_quote_cache 表：旧表 code 无主键，无法使用 ON CONFLICT(code)，
// 先复制数据到带主键的新表再切换（缓存表，数据可随时由行情源恢复）。
func (c *QuoteCache) migrateTable() error {
	log.Printf("[QuoteCache] cn_quote_cache 缺少主键，重建表结构（保留已有数据）")
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	steps := []string{
		`CREATE TABLE IF NOT EXISTS cn_quote_cache_new (
    code        VARCHAR(16) PRIMARY KEY,
    name        VARCHAR(64),
    price       FLOAT,
    open        FLOAT,
    prev_close  FLOAT,
    high        FLOAT,
    low         FLOAT,
    price_ts    BIGINT,
    updated_at  BIGINT
)`,
		`INSERT OR IGNORE INTO cn_quote_cache_new
	(code,name,price,open,prev_close,high,low,price_ts,updated_at)
	SELECT code,name,price,open,prev_close,high,low,price_ts,updated_at
	FROM cn_quote_cache`,
		`DROP TABLE cn_quote_cache`,
		`ALTER TABLE cn_quote_cache_new RENAME TO cn_quote_cache`,
	}
	for _, s := range steps {
		if _, err := tx.Exec(s); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	log.Printf("[QuoteCache] cn_quote_cache 表结构已重建（主键 code）")
	return nil
}

// UpsertMany 批量写入（来自批量行情结果）：先更新内存，再落库
func (c *QuoteCache) UpsertMany(records map[string]*QuoteRecord) {
	now := time.Now().Unix()
	rows := make([]quoteRow, 0, len(records))

	c.mu.Lock()
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
		rows = append(rows, quoteRow{
			code:      key,
			name:      r.Name,
			price:     r.Price,
			open:      r.Open,
			prevClose: r.PrevClose,
			high:      nullableFloat(r.High),
			low:       nullableFloat(r.Low),
			priceTS:   r.PriceTS,
			updatedAt: now,
		})
	}
	c.mu.Unlock()

	// 锁外落库，避免长时间持锁阻塞读取
	if c.db != nil && len(rows) > 0 {
		c.upsertRows(rows)
	}
}

// nullableFloat 将非正数转为 NULL（与 Python 版 high/low 可为 None 一致）
func nullableFloat(v float64) *float64 {
	if v <= 0 {
		return nil
	}
	return &v
}

// upsertRows 事务批量 upsert 到 cn_quote_cache（SQL 与 Python 版一致）
func (c *QuoteCache) upsertRows(rows []quoteRow) {
	stmtText := `INSERT INTO cn_quote_cache
	(code,name,price,open,prev_close,high,low,price_ts,updated_at)
	VALUES (?,?,?,?,?,?,?,?,?)
	ON CONFLICT(code) DO UPDATE SET
	name=excluded.name, price=excluded.price, open=excluded.open,
	prev_close=excluded.prev_close, high=excluded.high, low=excluded.low,
	price_ts=excluded.price_ts, updated_at=excluded.updated_at`
	tx, err := c.db.Begin()
	if err != nil {
		log.Printf("[QuoteCache] 写库事务开始失败: %v", err)
		return
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(stmtText)
	if err != nil {
		log.Printf("[QuoteCache] 写库准备失败: %v", err)
		return
	}
	defer stmt.Close()

	for _, r := range rows {
		if _, err := stmt.Exec(r.code, r.name, r.price, r.open, r.prevClose,
			r.high, r.low, r.priceTS, r.updatedAt); err != nil {
			log.Printf("[QuoteCache] 写库失败(code=%s): %v", r.code, err)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		log.Printf("[QuoteCache] 写库提交失败: %v", err)
	}
}

// loadAllQuotes 从 DB 读取全部缓存，预热内存（启动时调用）
func (c *QuoteCache) loadAllQuotes() (int, error) {
	rows, err := c.db.Query(`SELECT code,name,price,open,prev_close,high,low,price_ts,updated_at
		FROM cn_quote_cache`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	n := 0
	c.mu.Lock()
	defer c.mu.Unlock()
	for rows.Next() {
		var e QuoteCacheEntry
		var high, low sql.NullFloat64
		if err := rows.Scan(&e.Code, &e.Name, &e.Price, &e.Open, &e.PrevClose,
			&high, &low, &e.PriceTS, &e.UpdatedAt); err != nil {
			continue
		}
		if high.Valid {
			e.High = high.Float64
		}
		if low.Valid {
			e.Low = low.Float64
		}
		c.m[e.Code] = &e
		n++
	}
	return n, rows.Err()
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
