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

	natsdiscovery "github.com/stock-server/nats-discovery"
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
	JetStreamURL        string
	JetStreamEnabled    bool
	JetStreamDiscover   bool
}

// DefaultConfig 返回基于 flag 默认值 + env 默认值的等价配置
func DefaultConfig() ServerConfig {
	return ServerConfig{
		Host:                "0.0.0.0",
		Port:                8080,
		SyncInterval:        60,
		UseTDX:              true,
		TDXPort:             7709,
		DBPath:              "../stock/instockdb.sqlite3",
		EnableMQTT:          true,
		MQTTBroker:          "wss://broker.emqx.io:8084/mqtt",
		MQTTTopic:           "secure/stock-price-mqtt",
		MQTTPassword:        "stock_price_mqtt_S3cret",
		MQTTPushInterval:    5,
		JetStreamURL:        "nats://127.0.0.1:4222",
		JetStreamEnabled:    false,
		JetStreamDiscover:   false,
	}
}

// RunningServer 表示一个非阻塞启动的实例（方案 B / C 用）
type RunningServer struct {
	Addr       string
	HTTPServer *http.Server
	Stop       func() error
	discovery  *ServiceDiscovery
}

// DiscoveryStatus 返回服务发现的当前状态（供 GUI 调试用）
func (rs *RunningServer) DiscoveryStatus() DiscoveryStatus {
	if rs == nil || rs.discovery == nil {
		return DiscoveryStatus{}
	}
	return rs.discovery.Status()
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
		jetStreamURL        string
		jetStreamEnabled    bool
		jetStreamDiscover   bool
	)

	fs := flag.NewFlagSet("go-stock-server", flag.ContinueOnError)
	fs.BoolVar(&debug, "debug", getEnvBool("DEBUG", false), "启用 DEBUG 模式")
	fs.StringVar(&host, "host", getEnv("HOST", "0.0.0.0"), "监听地址")
	fs.IntVar(&port, "port", getEnvInt("PORT", 8080), "监听端口")
	fs.IntVar(&syncInterval, "sync-interval", getEnvInt("SYNC_INTERVAL", 60), "配置同步间隔（秒），0=禁用")
	fs.BoolVar(&useTdx, "tdx", getEnvBool("TDX_ENABLED", true), "启用通达信 TCP 数据源（自动测速选最优，默认开启，-tdx=false 关闭）")
	fs.StringVar(&tdxHost, "tdx-host", getEnv("TDX_HOST", ""), "通达信服务器地址")
	fs.IntVar(&tdxPort, "tdx-port", getEnvInt("TDX_PORT", 7709), "通达信服务器端口")
	fs.StringVar(&quoteDBPath, "db", getEnv("DB_PATH", "../stock/instockdb.sqlite3"), "SQLite 路径（空=纯内存）")

	mqttEnabled = getEnvBool("MQTT_ENABLED", true)
	mqttBroker = getEnv("MQTT_BROKER", "wss://broker.emqx.io:8084/mqtt")
	mqttTopic = getEnv("MQTT_TOPIC", "secure/stock-price-mqtt")
	mqttPassword = getEnv("MQTT_PASSWORD", "stock_price_mqtt_S3cret")
	mqttClientID = getEnv("MQTT_CLIENT_ID", "")
	mqttUserSuffix = getEnv("MQTT_USER_SUFFIX", "")
	mqttPushInterval = getEnvInt("MQTT_PUSH_INTERVAL", 5)
	mqttPushOnlyTrading = getEnvBool("MQTT_PUSH_ONLY_TRADING", false)
	jetStreamURL = getEnv("JETSTREAM_URL", "nats://127.0.0.1:4222")
	jetStreamEnabled = getEnvBool("JETSTREAM_ENABLED", false)
	jetStreamDiscover = getEnvBool("JETSTREAM_DISCOVER", false)

	fs.BoolVar(&mqttEnabled, "mqtt", mqttEnabled, "启用 MQTT 接入层（默认开启，-mqtt=false 关闭）")
	fs.StringVar(&mqttBroker, "mqtt-broker", mqttBroker, "MQTT broker 地址")
	fs.StringVar(&mqttTopic, "mqtt-topic", mqttTopic, "MQTT 订阅/发布 Topic")
	fs.StringVar(&mqttPassword, "mqtt-password", mqttPassword, "MQTT AES 密码")
	fs.StringVar(&mqttClientID, "mqtt-client-id", mqttClientID, "MQTT clientId")
	fs.StringVar(&mqttUserSuffix, "mqtt-user-suffix", mqttUserSuffix, "附加 ping user 后缀")
	fs.IntVar(&mqttPushInterval, "mqtt-push-interval", mqttPushInterval, "实时价推送间隔(秒)")
	fs.BoolVar(&mqttPushOnlyTrading, "mqtt-push-only-trading", mqttPushOnlyTrading, "仅交易时段推送")
	fs.StringVar(&jetStreamURL, "jetstream-url", jetStreamURL, "JetStream/NATS 备服务器地址（MQTT 不可用时故障转移目标）")
	fs.BoolVar(&jetStreamEnabled, "jetstream", jetStreamEnabled, "启用 JetStream 备服务器故障转移（默认关闭）")
	fs.BoolVar(&jetStreamDiscover, "jetstream-discover", jetStreamDiscover, "通过 mDNS 自动发现 NATS/JetStream 服务器（覆盖 jetstream-url 并强制启用故障转移）")

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
		JetStreamURL:        jetStreamURL,
		JetStreamEnabled:    jetStreamEnabled,
		JetStreamDiscover:   jetStreamDiscover,
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

	// 通过 mDNS 自动发现 NATS/JetStream 服务器（覆盖 jetstream-url 并强制启用故障转移）。
	if cfg.JetStreamDiscover {
		log.Println("[启动] 通过 mDNS 自动发现 NATS/JetStream 服务器...")
		urls, err := natsdiscovery.Discover(3 * time.Second)
		if err != nil || len(urls) == 0 {
			log.Printf("[启动] ⚠️ mDNS 未发现 NATS 服务器，JetStream 故障转移将禁用（仅 MQTT）: %v", err)
			cfg.JetStreamEnabled = false
		} else {
			log.Printf("[启动] ✅ mDNS 发现 NATS 服务器: %v", urls)
			cfg.JetStreamURL = urls[0]
			cfg.JetStreamEnabled = true
		}
	}

	log.Println("[启动 step 1/8] 开始初始化 StockFetcher")
	if cfg.Debug {
		log.Println("DEBUG 模式已启用，将显示详细请求/响应日志")
	}

	fetcher := NewStockFetcher(cfg.Debug)
	log.Println("[启动 step 1/8] StockFetcher 完成")

	var tdxDS *TdxDataSource
	if cfg.UseTDX {
		if cfg.TDXPort == 0 {
			cfg.TDXPort = 7709
		}
		log.Println("[启动 step 2/8] 初始化 TDX 数据源...")
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
		log.Println("[启动 step 2/8] TDX 数据源完成")
		// 启动后台自动重连：TDX 未连接时周期性重测速，等待可用服务器窗口。
		if tdxDS != nil {
			tdxDS.StartAutoReconnect()
		}
	} else {
		log.Println("[启动 step 2/8] TDX 未启用，跳过")
	}

	log.Println("[启动 step 3/8] 初始化 NodeConfigStore")
	nodeStore := NewNodeConfigStore(cfg.SyncInterval)
	if cfg.SyncInterval > 0 {
		log.Printf("配置同步间隔: 每 %d 秒自动同步", cfg.SyncInterval)
	} else {
		log.Println("配置同步: 已禁用")
	}
	log.Println("[启动 step 3/8] NodeConfigStore 完成")

	log.Printf("[启动 step 4/8] 初始化 QuoteCache (dbPath=%q)", cfg.DBPath)
	quoteCache := NewQuoteCache(cfg.DBPath)
	log.Println("[启动 step 4/8] QuoteCache 完成")

	log.Println("[启动 step 5/8] 初始化 ServiceDiscovery (后台异步)")
	discovery := NewServiceDiscovery(cfg.Port)
	// 非阻塞：mDNS/UDP 发现后台启动，不延迟 HTTP 服务
	go discovery.Start()
	log.Println("[启动 step 5/8] ServiceDiscovery 已提交后台启动")

	log.Println("[启动 step 6/8] 注册 HTTP 路由")
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
	log.Println("[启动 step 6/8] 路由注册完成")

	var mqttClient *MQTTPriceClient
	if cfg.EnableMQTT {
		// 调用方（GUI / 方案 B、C）可能只设了 EnableMQTT 而漏传连接参数，
		// 这里统一补齐默认值，避免连空 broker。
		if cfg.MQTTBroker == "" {
			cfg.MQTTBroker = "wss://broker.emqx.io:8084/mqtt"
		}
		if cfg.MQTTTopic == "" {
			cfg.MQTTTopic = "secure/stock-price-mqtt"
		}
		if cfg.MQTTPassword == "" {
			cfg.MQTTPassword = "stock_price_mqtt_S3cret"
		}
		if cfg.MQTTPushInterval < 1 {
			cfg.MQTTPushInterval = 5
		}
		log.Printf("[启动 step 7/8] 初始化 MQTT... (broker=%s topic=%s)", cfg.MQTTBroker, cfg.MQTTTopic)
		mqttClient = NewMQTTClient(handler, cfg.MQTTBroker, cfg.MQTTTopic, cfg.MQTTPassword,
			cfg.MQTTClientID, cfg.MQTTUserSuffix, cfg.MQTTPushInterval, cfg.MQTTPushOnlyTrading,
			cfg.JetStreamURL, cfg.JetStreamEnabled)
		mqttClient.Start()
		log.Println("[启动 step 7/8] MQTT 完成")

		// 持续监听 NATS/JetStream 服务器：若其 IP 变化（如休眠唤醒换 DHCP 地址），
		// 自动把备服务器地址重新指向新地址，保证故障转移目标始终可达。
		if cfg.JetStreamDiscover && cfg.JetStreamEnabled {
			go func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				_ = natsdiscovery.Watch(ctx, func(urls []string) {
					if len(urls) > 0 {
						mqttClient.SetBackupURL(urls[0])
					}
				})
			}()
		}
	} else {
		log.Println("[启动 step 7/8] MQTT 未启用，跳过")
	}

	httpSrv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler:      withCORS(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Println("[启动 step 8/8] 启动 HTTP 服务器 (ListenAndServe)")
	// localIP 探测放到 goroutine 中，不阻塞 HTTP 启动
	// Android 无外网时 net.Dial 可能阻塞，这里仅用于日志展示
	go func() {
		localIP := getLocalIP()
		log.Printf("[启动] 本机 IP 探测完成: %s", localIP)
		log.Printf("股票行情服务器启动 (Go 版) v%s", serverVersion)
		log.Printf("  本机 IP: %s", localIP)
		log.Printf("  HTTP 端口: %d", cfg.Port)
		log.Printf("  访问地址: http://%s:%d", localIP, cfg.Port)
		if cfg.Debug {
			log.Println("DEBUG 模式: 已启用 (--debug)")
		} else {
			log.Println("DEBUG 模式: 未启用 (--debug 启用)")
		}
		log.Println("服务发现: mDNS/UDP 后台启动中（见后续日志）")
	}()

	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[HTTP] 错误退出: %v", err)
		}
	}()

	rs := &RunningServer{
		Addr:       httpSrv.Addr,
		HTTPServer: httpSrv,
		discovery:  discovery,
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
