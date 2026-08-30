// go-stock-server/internal/core/main.go - 核心服务器入口（供 cmd/server / 方案 B、C 调用）
package core

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// ServerConfig 是干净的配置结构（方案 B / C 首选）
type ServerConfig struct {
	Debug               bool
	Host                string
	Port                int
	SyncInterval        int
	UseTDX              bool
	TDXHost             string
	TDXPort             int
	DBPath              string
	EnableMQTT          bool
	MQTTBroker          string
	MQTTTopic           string
	MQTTPassword        string
	MQTTClientID        string
	MQTTUserSuffix      string
	MQTTPushInterval    int
	MQTTPushOnlyTrading bool
}

// DefaultConfig 返回基于 flag 默认值 + env 默认值的等价配置
func DefaultConfig() ServerConfig {
	return ServerConfig{
		Host:                "0.0.0.0",
		Port:                8080,
		SyncInterval:        60,
		DBPath:              "../stock/instockdb.sqlite3",
		MQTTBroker:          "wss://broker.emqx.io:8084/mqtt",
		MQTTTopic:           "secure/stock-price-mqtt",
		MQTTPassword:        "stock_price_mqtt_S3cret",
		MQTTPushInterval:    5,
	}
}

// RunningServer 表示一个非阻塞启动的实例（方案 B / C 用）
type RunningServer struct {
	Addr       string
	HTTPServer *http.Server
	Stop       func() error
}

// Version 返回服务器版本号
func Version() string { return serverVersion }

// RunLegacyMain 以原 main() 的 1:1 行为运行（CLI/方案A用）
func RunLegacyMain(args []string, block bool) (*RunningServer, error) {
	var (
		debug               bool
		host                string
		port                int
		syncInterval        int
		useTdx              bool
		tdxHost             string
		tdxPort             int
		quoteDBPath         string
		mqttEnabled         bool
		mqttBroker          string
		mqttTopic           string
		mqttPassword        string
		mqttClientID        string
		mqttUserSuffix      string
		mqttPushInterval    int
		mqttPushOnlyTrading bool
	)

	fs := flag.NewFlagSet("go-stock-server", flag.ContinueOnError)
	fs.BoolVar(&debug, "debug", getEnvBool("DEBUG", false), "启用 DEBUG 模式")
	fs.StringVar(&host, "host", getEnv("HOST", "0.0.0.0"), "监听地址")
	fs.IntVar(&port, "port", getEnvInt("PORT", 8080), "监听端口")
	fs.IntVar(&syncInterval, "sync-interval", getEnvInt("SYNC_INTERVAL", 60), "配置同步间隔（秒），0=禁用")
	fs.BoolVar(&useTdx, "tdx", getEnvBool("TDX_ENABLED", false), "启用通达信 TCP 数据源")
	fs.StringVar(&tdxHost, "tdx-host", getEnv("TDX_HOST", ""), "通达信服务器地址")
	fs.IntVar(&tdxPort, "tdx-port", getEnvInt("TDX_PORT", 7709), "通达信服务器端口")
	fs.StringVar(&quoteDBPath, "db", getEnv("DB_PATH", "../stock/instockdb.sqlite3"), "SQLite 路径（空=纯内存）")

	mqttEnabled = getEnvBool("MQTT_ENABLED", false)
	mqttBroker = getEnv("MQTT_BROKER", "wss://broker.emqx.io:8084/mqtt")
	mqttTopic = getEnv("MQTT_TOPIC", "secure/stock-price-mqtt")
	mqttPassword = getEnv("MQTT_PASSWORD", "stock_price_mqtt_S3cret")
	mqttClientID = getEnv("MQTT_CLIENT_ID", "")
	mqttUserSuffix = getEnv("MQTT_USER_SUFFIX", "")
	mqttPushInterval = getEnvInt("MQTT_PUSH_INTERVAL", 5)
	mqttPushOnlyTrading = getEnvBool("MQTT_PUSH_ONLY_TRADING", false)

	fs.BoolVar(&mqttEnabled, "mqtt", mqttEnabled, "启用 MQTT 接入层")
	fs.StringVar(&mqttBroker, "mqtt-broker", mqttBroker, "MQTT broker 地址")
	fs.StringVar(&mqttTopic, "mqtt-topic", mqttTopic, "MQTT 订阅/发布 Topic")
	fs.StringVar(&mqttPassword, "mqtt-password", mqttPassword, "MQTT AES 密码")
	fs.StringVar(&mqttClientID, "mqtt-client-id", mqttClientID, "MQTT clientId")
	fs.StringVar(&mqttUserSuffix, "mqtt-user-suffix", mqttUserSuffix, "附加 ping user 后缀")
	fs.IntVar(&mqttPushInterval, "mqtt-push-interval", mqttPushInterval, "实时价推送间隔(秒)")
	fs.BoolVar(&mqttPushOnlyTrading, "mqtt-push-only-trading", mqttPushOnlyTrading, "仅交易时段推送")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	cfg := ServerConfig{
		Debug:               debug,
		Host:                host,
		Port:                port,
		SyncInterval:        syncInterval,
		UseTDX:              useTdx,
		TDXHost:             tdxHost,
		TDXPort:             tdxPort,
		DBPath:              quoteDBPath,
		EnableMQTT:          mqttEnabled,
		MQTTBroker:          mqttBroker,
		MQTTTopic:           mqttTopic,
		MQTTPassword:        mqttPassword,
		MQTTClientID:        mqttClientID,
		MQTTUserSuffix:      mqttUserSuffix,
		MQTTPushInterval:    mqttPushInterval,
		MQTTPushOnlyTrading: mqttPushOnlyTrading,
	}
	return startFromConfig(cfg, block)
}

// StartServer 以结构化配置启动（方案 B / C 推荐）
func StartServer(cfg ServerConfig, block bool) (*RunningServer, error) {
	return startFromConfig(cfg, block)
}

// startFromConfig —— 真正的启动逻辑，与原 main() 行为 1:1
func startFromConfig(cfg ServerConfig, block bool) (*RunningServer, error) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	if cfg.Debug {
		log.Println("DEBUG 模式已启用，将显示详细请求/响应日志")
	}

	fetcher := NewStockFetcher(cfg.Debug)

	var tdxDS *TdxDataSource
	if cfg.UseTDX {
		if cfg.TDXHost != "" {
			tdxDS = NewTdxDataSource(false, cfg.Debug)
			if err := tdxDS.provider.ConnectTo(cfg.TDXHost, cfg.TDXPort); err != nil {
				log.Printf("[TDX] ⚠️ 连接指定服务器 %s:%d 失败: %v", cfg.TDXHost, cfg.TDXPort, err)
			} else {
				tdxDS.enabled = true
				log.Printf("[TDX] ✅ 已连接 %s:%d", cfg.TDXHost, cfg.TDXPort)
			}
		} else {
			tdxDS = NewTdxDataSource(true, cfg.Debug)
			if !tdxDS.Enabled() {
				log.Println("[TDX] ⚠️ 自动连接失败，回退到腾讯 HTTP 数据源")
			}
		}
	}

	nodeStore := NewNodeConfigStore(cfg.SyncInterval)
	if cfg.SyncInterval > 0 {
		log.Printf("配置同步间隔: 每 %d 秒自动同步", cfg.SyncInterval)
	} else {
		log.Println("配置同步: 已禁用")
	}

	quoteCache := NewQuoteCache(cfg.DBPath)

	discovery := NewServiceDiscovery(cfg.Port)
	discovery.Start()

	handler := NewStockHandler(fetcher, tdxDS, nodeStore, quoteCache, cfg.Debug)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", handler.HandleHealth)
	mux.HandleFunc("/api/config", handler.HandleConfig)
	mux.HandleFunc("/api/node/config", handler.HandleNodeConfig)
	mux.HandleFunc("/api/realtime/", handler.HandleRealtime)
	mux.HandleFunc("/api/kline/", handler.HandleKline)
	mux.HandleFunc("/api/qfq/", handler.HandleQfq)
	mux.HandleFunc("/api/batch/quotes", handler.HandleBatchQuotes)
	mux.HandleFunc("/api/quote_cache", handler.HandleQuoteCache)
	mux.HandleFunc("/api/name/", handler.HandleName)
	mux.HandleFunc("/api/minute/", handler.HandleMinute)
	mux.HandleFunc("/api/valuation/", handler.HandleValuation)
	mux.HandleFunc("/api/intraday/", handler.HandleIntraday)

	var mqttClient *MQTTPriceClient
	if cfg.EnableMQTT {
		mqttClient = NewMQTTClient(handler, cfg.MQTTBroker, cfg.MQTTTopic, cfg.MQTTPassword,
			cfg.MQTTClientID, cfg.MQTTUserSuffix, cfg.MQTTPushInterval, cfg.MQTTPushOnlyTrading)
		mqttClient.Start()
	}

	httpSrv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler:      withCORS(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	localIP := getLocalIP()
	log.Printf("股票行情服务器启动 (Go 版) v%s", serverVersion)
	log.Printf("  本机 IP: %s", localIP)
	log.Printf("  HTTP 端口: %d", cfg.Port)
	log.Printf("  访问地址: http://%s:%d", localIP, cfg.Port)
	if cfg.Debug {
		log.Println("DEBUG 模式: 已启用 (--debug)")
	} else {
		log.Println("DEBUG 模式: 未启用 (--debug 启用)")
	}
	discovery.PrintInfo(localIP)
	log.Println("接口列表:")
	log.Println("  - /api/health - 健康检查")
	log.Println("  - /api/config - 服务器配置信息")
	log.Println("  - /api/node/config - 节点配置管理 (GET/POST/DELETE)")
	log.Println("  - /api/realtime/<code> - 实时行情")
	log.Println("  - /api/kline/<code>?days=30 - K线数据")
	log.Println("  - /api/qfq/<code>?days=30 - 前复权K线")
	log.Println("  - /api/batch/quotes?codes=000001,600000 - 批量实时行情")
	log.Println("  - /api/quote_cache - 实时价格缓存")
	log.Println("  - /api/name/<code> - 股票名称")
	log.Println("  - /api/minute/<code>?period=7&minutes=300 - 分钟K线")
	log.Println("  - /api/intraday/<code>?date=YYYYMMDD - 分时数据")
	log.Println("  - /api/valuation/<code>?days=250 - 估值历史")

	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[HTTP] 错误退出: %v", err)
		}
	}()

	rs := &RunningServer{
		Addr:       httpSrv.Addr,
		HTTPServer: httpSrv,
	}
	rs.Stop = func() error {
		log.Println("服务器停止")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if tdxDS != nil {
			tdxDS.Close()
		}
		if mqttClient != nil {
			mqttClient.Stop()
		}
		discovery.Stop()
		quoteCache.Close()
		return httpSrv.Shutdown(ctx)
	}

	if block {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		return rs, rs.Stop()
	}
	return rs, nil
}

// ===== 辅助函数：环境变量解析 =====

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
func getEnvBool(key string, def bool) bool {
	switch strings.ToLower(os.Getenv(key)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return def
}
func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}

// ===== CORS 中间件（原 main.go 的 withCORS，原样保留） =====

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Node-ID, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
