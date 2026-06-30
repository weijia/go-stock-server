// go-stock-server/main.go - 股票行情 HTTP 服务器入口
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	debug       bool
	host        string
	port        int
	syncInterval int
	useTdx      bool
	tdxHost     string
	tdxPort     int
)

func main() {
	flag.BoolVar(&debug, "debug", getEnvBool("DEBUG", false), "启用 DEBUG 模式")
	flag.StringVar(&host, "host", getEnv("HOST", "0.0.0.0"), "监听地址")
	flag.IntVar(&port, "port", getEnvInt("PORT", 8080), "监听端口")
	flag.IntVar(&syncInterval, "sync-interval", getEnvInt("SYNC_INTERVAL", 60), "配置同步间隔（秒），0=禁用")
	flag.BoolVar(&useTdx, "tdx", getEnvBool("TDX_ENABLED", false), "启用通达信 TCP 数据源（自动测速选最优）")
	flag.StringVar(&tdxHost, "tdx-host", getEnv("TDX_HOST", ""), "通达信服务器地址（指定后跳过自动测速）")
	flag.IntVar(&tdxPort, "tdx-port", getEnvInt("TDX_PORT", 7709), "通达信服务器端口，默认 7709")
	flag.Parse()

	// 日志设置
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	if debug {
		log.Println("DEBUG 模式已启用，将显示详细请求/响应日志")
	}

	// 创建数据获取器
	fetcher := NewStockFetcher(debug)

	// 通达信 TCP 数据源（可选）
	var tdxDS *TdxDataSource
	if useTdx {
		if tdxHost != "" {
			tdxDS = NewTdxDataSource(false, debug)
			err := tdxDS.provider.ConnectTo(tdxHost, tdxPort)
			if err != nil {
				log.Printf("[TDX] ⚠️ 连接指定服务器 %s:%d 失败: %v\n", tdxHost, tdxPort, err)
			} else {
				tdxDS.enabled = true
				log.Printf("[TDX] ✅ 已连接 %s:%d\n", tdxHost, tdxPort)
			}
		} else {
			tdxDS = NewTdxDataSource(true, debug)
			if !tdxDS.Enabled() {
				log.Println("[TDX] ⚠️ 自动连接失败，回退到腾讯 HTTP 数据源")
			}
		}
	}

	// 创建节点配置存储
	nodeStore := NewNodeConfigStore(syncInterval)
	if syncInterval > 0 {
		log.Printf("配置同步间隔: 每 %d 秒自动同步\n", syncInterval)
	} else {
		log.Println("配置同步: 已禁用")
	}

	// 创建服务发现
	discovery := NewServiceDiscovery(port)
	discovery.Start()

	// 创建 HTTP 服务
	handler := NewStockHandler(fetcher, tdxDS, nodeStore, debug)
	mux := http.NewServeMux()

	// 注册路由
	mux.HandleFunc("/api/health", handler.HandleHealth)
	mux.HandleFunc("/api/config", handler.HandleConfig)
	mux.HandleFunc("/api/node/config", handler.HandleNodeConfig)
	mux.HandleFunc("/api/realtime/", handler.HandleRealtime)
	mux.HandleFunc("/api/kline/", handler.HandleKline)
	mux.HandleFunc("/api/intraday/", handler.HandleIntraday)

	server := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", host, port),
		Handler:      withCORS(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 打印启动信息
	localIP := getLocalIP()
	log.Println("股票行情服务器启动 (Go 版)")
	log.Printf("  本机 IP: %s\n", localIP)
	log.Printf("  HTTP 端口: %d\n", port)
	log.Printf("  访问地址: http://%s:%d\n", localIP, port)
	if debug {
		log.Println("DEBUG 模式: 已启用 (--debug)")
	} else {
		log.Println("DEBUG 模式: 未启用 (--debug 启用)")
	}
	discovery.PrintInfo(localIP)

	log.Println("接口列表:")
	log.Println("  - /api/health - 健康检查")
	log.Println("  - /api/config - 服务器配置信息")
	log.Println("  - /api/node/config - 节点配置管理 (GET/POST/DELETE)")
	log.Println("  - /api/realtime/<code> - 实时行情（示例：000001）")
	log.Println("  - /api/kline/<code>?days=30 - K线数据（示例：000001）")
	log.Println("  - /api/intraday/<code>?date=YYYYMMDD - 分时数据（示例：000001）")

	// 优雅退出
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("服务器停止")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if tdxDS != nil {
			tdxDS.Close()
		}
		discovery.Stop()
		server.Shutdown(ctx)
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("服务器错误: %v", err)
	}
}

// withCORS CORS 中间件
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Node-ID")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v == "true" || v == "1" || v == "yes"
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var result int
	fmt.Sscanf(v, "%d", &result)
	return result
}
