// 股票行情服务器方案 C GUI 主入口（跨平台桌面 + Android）
// 桌面用法（需 CGO OpenGL）:
//   go run ./cmd/android-gui
// Android APK:
//   fyne package -os android -appID com.stock.server.gui -name StockServerGUI -icon <PNG> ./cmd/android-gui
//   （若 fyne 在子目录找 main 失败，可把本文件复制到 module 根目录后用「.」做目标）

package main

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/data/binding"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"

	"go-stock-server/internal/core"
)

type guiApp struct {
	app     fyne.App
	win     fyne.Window
	cfg     core.ServerConfig
	running binding.Bool

	server   *core.RunningServer
	starting bool // 标记正在启动中，防止重复点击
	mu       sync.Mutex

	// 跨线程 UI 请求：goroutine 设置标志，动画定时器在 UI 线程处理
	pendingRefresh bool   // true=需要刷新运行状态
	pendingDialog  string // 非空=需要显示错误对话框

	portEntry *widget.Entry
	syncEntry *widget.Entry
	swTdx     *widget.Check
	swMqtt    *widget.Check
	swDebug   *widget.Check
	statusLbl *widget.Label
	ipLbl     *widget.Label
	mdnsLbl   *widget.Label // mDNS 状态
	startBtn  *widget.Button
	stopBtn   *widget.Button

	codeEntry  *widget.Entry
	multiEntry *widget.Entry
	resultBox  *widget.Label
	fetcher    *core.StockFetcher

	logView *widget.Entry
	logBuf  strings.Builder
	logMu   sync.Mutex
}

// guiLogWriter 把标准 log 包的输出重定向到 GUI 日志 Tab
type guiLogWriter struct{ g *guiApp }

func (w *guiLogWriter) Write(p []byte) (int, error) {
	line := strings.TrimRight(string(p), "\n\r")
	if line != "" {
		w.g.appendLog(line)
	}
	return len(p), nil
}

func main() {
	g := &guiApp{
		app:     app.NewWithID("com.stock.server.gui"),
		cfg:     core.DefaultConfig(),
		running: binding.NewBool(),
	}
	// 关键：把标准 log 输出重定向到 GUI 日志 Tab
	// 这样 ServiceDiscovery / StartServer 的所有 log.Println 都会在 GUI 里看到
	log.SetOutput(&guiLogWriter{g: g})
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	g.app.Settings().SetTheme(theme.DefaultTheme())
	g.win = g.app.NewWindow("📈 股票行情服务器 v" + g.version())
	g.win.Resize(fyne.NewSize(420, 720))
	g.win.SetMaster()

	g.fetcher = core.NewStockFetcher(false)
	g.buildTabs()
	// 启动 UI 线程定时刷新：在 Android 上从 goroutine 调用 SetText 会死锁
	// GL 渲染线程，导致 log 包互斥锁无法释放 → 后续所有 log.Println 调用永久
	// 阻塞 → 启动超时 30s。
	// 解决方案：goroutine 只写缓冲区 + 设置 pending 标志，
	// Fyne 动画在 UI 线程上周期性检查并刷新 UI。
	(&fyne.Animation{
		Duration:    200 * time.Millisecond,
		RepeatCount: fyne.AnimationRepeatForever, // 无限循环
		Tick: func(v float32) {
			// 1. 刷新日志
			g.logMu.Lock()
			txt := g.logBuf.String()
			g.logMu.Unlock()
			if g.logView != nil && g.logView.Text != txt {
				g.logView.SetText(txt)
			}
			// 2. 处理 UI 待刷新请求（来自 goroutine 的 running.Set）
			g.mu.Lock()
			needRefresh := g.pendingRefresh
			g.pendingRefresh = false
			g.mu.Unlock()
			if needRefresh {
				r, _ := g.running.Get()
				g.refreshRunState(r)
			}
			// 3. 处理待显示错误对话框
			g.mu.Lock()
			pendingDialog := g.pendingDialog
			g.pendingDialog = ""
			g.mu.Unlock()
			if pendingDialog != "" {
				dialog.ShowError(fmt.Errorf("%s", pendingDialog), g.win)
			}
		},
	}).Start()
	// running.Set 的监听器：从 goroutine 调用时不能直接刷新 UI，
	// 设置 pendingRefresh 标志，由动画定时器在 UI 线程处理。
	g.running.AddListener(binding.NewDataListener(func() {
		g.mu.Lock()
		g.pendingRefresh = true
		g.mu.Unlock()
	}))
	_ = g.running.Set(false)
	g.win.SetCloseIntercept(func() { g.stopServer(); g.win.Close() })
	g.win.ShowAndRun()
}

func (g *guiApp) refreshRunState(r bool) {
	if r {
		g.statusLbl.SetText("✅ 运行中")
		g.statusLbl.Importance = widget.SuccessImportance
		g.ipLbl.SetText(fmt.Sprintf("本机 IP：%s    端口：%d", core.GetLocalIP(), g.cfg.Port))
		g.refreshMDNSStatus()
	} else {
		g.statusLbl.SetText("⏸ 已停止")
		g.statusLbl.Importance = widget.DangerImportance
		g.ipLbl.SetText("")
		g.mdnsLbl.SetText("mDNS: 未启动")
	}
	if r {
		g.startBtn.Disable()
		g.stopBtn.Enable()
	} else {
		g.startBtn.Enable()
		g.stopBtn.Disable()
	}
}

// refreshMDNSStatus 从 RunningServer 获取 mDNS 状态并更新 UI
func (g *guiApp) refreshMDNSStatus() {
	g.mu.Lock()
	rs := g.server
	g.mu.Unlock()
	if rs == nil {
		g.mdnsLbl.SetText("mDNS: 未启动")
		return
	}
	st := rs.DiscoveryStatus()
	var mode string
	switch {
	case !st.Running:
		mode = "未启动"
	case st.AnnounceOnly:
		mode = "仅宣告"
	default:
		mode = "完整模式"
	}
	g.mdnsLbl.SetText(fmt.Sprintf("mDNS: %s | IP:%s | 宣告:%d | 查询:%d",
		mode, st.LocalIP, st.AnnounceCount, st.QueryCount))
}

func (g *guiApp) buildTabs() {
	tabs := container.NewAppTabs(
		container.NewTabItem("⚙️ 服务", g.buildSettingsTab()),
		container.NewTabItem("📊 行情", g.buildQuoteTab()),
		container.NewTabItem("📝 日志", g.buildLogTab()),
	)
	tabs.SetTabLocation(container.TabLocationTop)
	g.win.SetContent(tabs)
}

// -------- Tab 1: 服务设置 --------
func (g *guiApp) buildSettingsTab() *fyne.Container {
	g.portEntry = widget.NewEntry()
	g.portEntry.SetPlaceHolder("端口 (1024-65535)")
	g.portEntry.SetText(fmt.Sprintf("%d", g.cfg.Port))
	g.syncEntry = widget.NewEntry()
	g.syncEntry.SetPlaceHolder("同步间隔（秒）")
	g.syncEntry.SetText(fmt.Sprintf("%d", g.cfg.SyncInterval))
	g.swTdx = widget.NewCheck("启用通达信 TCP 数据源", func(c bool) { g.cfg.UseTDX = c })
	g.swMqtt = widget.NewCheck("启用 MQTT 接入层", func(c bool) { g.cfg.EnableMQTT = c })
	g.swDebug = widget.NewCheck("DEBUG 详细日志", func(c bool) { g.cfg.Debug = c })
	g.swTdx.SetChecked(g.cfg.UseTDX)
	g.swMqtt.SetChecked(g.cfg.EnableMQTT)
	g.swDebug.SetChecked(g.cfg.Debug)
	form := widget.NewForm(
		widget.NewFormItem("HTTP 端口", g.portEntry),
		widget.NewFormItem("同步间隔", g.syncEntry),
		widget.NewFormItem("", g.swTdx),
		widget.NewFormItem("", g.swMqtt),
		widget.NewFormItem("", g.swDebug),
	)
	g.statusLbl = widget.NewLabel("⏸ 已停止")
	g.statusLbl.TextStyle = fyne.TextStyle{Bold: true}
	g.statusLbl.Importance = widget.DangerImportance
	g.ipLbl = widget.NewLabel("")
	g.mdnsLbl = widget.NewLabel("mDNS: 未启动")
	g.mdnsLbl.TextStyle = fyne.TextStyle{Monospace: true}
	g.mdnsLbl.Importance = widget.LowImportance
	g.startBtn = widget.NewButtonWithIcon("▶ 启动服务器", theme.MediaPlayIcon(), g.startServer)
	g.startBtn.Importance = widget.HighImportance
	g.stopBtn = widget.NewButtonWithIcon("■ 停止", theme.MediaStopIcon(), g.stopServer)
	g.stopBtn.Importance = widget.DangerImportance
	versionLbl := widget.NewLabel("📦 版本 v" + g.version())
	versionLbl.TextStyle = fyne.TextStyle{Monospace: true, Bold: true}
	versionLbl.Importance = widget.MediumImportance
	urlCard := widget.NewCard("浏览器 / 客户端访问", "", container.NewVBox(
		widget.NewLabel("同 WiFi 下访问："),
		widget.NewLabel("健康检查：  http://<本机IP>:端口/api/health"),
		widget.NewLabel("示例(茅台)：http://<本机IP>:端口/api/realtime/600519"),
		widget.NewLabel("批量：         /api/batch/quotes?codes=000001,601318,600519"),
	))
	return container.NewVBox(
		widget.NewCard("运行状态", "", container.NewVBox(g.statusLbl, g.ipLbl, g.mdnsLbl, versionLbl)),
		widget.NewCard("启动配置", "", form),
		container.NewGridWithColumns(2, g.startBtn, g.stopBtn),
		urlCard,
		layout.NewSpacer(),
	)
}

// -------- Tab 2: 行情 --------
func (g *guiApp) buildQuoteTab() *fyne.Container {
	g.codeEntry = widget.NewEntry()
	g.codeEntry.SetPlaceHolder("股票代码：000001 / 600519 / 510300")
	g.multiEntry = widget.NewEntry()
	g.multiEntry.SetPlaceHolder("批量：用英文逗号分隔，例如 000001,601318,600519")
	g.resultBox = widget.NewLabel("结果显示在这里（前 3000 字符）")
	g.resultBox.Wrapping = fyne.TextWrapWord
	rtBtn := widget.NewButton("▶ 查询单只实时", func() {
		c := strings.TrimSpace(g.codeEntry.Text)
		if c == "" {
			dialog.ShowInformation("提示", "请输入股票代码", g.win)
			return
		}
		g.appendLog(fmt.Sprintf("FetchRealtime(%s)", c))
		r, err := g.fetcher.FetchRealtime(c)
		if err != nil {
			g.resultBox.SetText("❌ " + err.Error())
			return
		}
		if r == nil {
			g.resultBox.SetText("❌ 结果为空")
			return
		}
		g.resultBox.SetText(renderOne(r))
	})
	batchBtn := widget.NewButton("▶ 批量查询", func() {
		raw := strings.Split(strings.ReplaceAll(g.multiEntry.Text, " ", ""), ",")
		cs := raw[:0]
		for _, s := range raw {
			if s != "" {
				cs = append(cs, s)
			}
		}
		if len(cs) == 0 {
			dialog.ShowInformation("提示", "请输入至少 1 个代码", g.win)
			return
		}
		g.appendLog(fmt.Sprintf("FetchBatchQuotes(%d codes)", len(cs)))
		m, err := g.fetcher.FetchBatchQuotes(cs)
		if err != nil {
			g.resultBox.SetText("❌ " + err.Error())
			return
		}
		g.resultBox.SetText(renderMany(m))
	})
	clearBtn := widget.NewButton("清空结果", func() { g.resultBox.SetText("") })
	return container.NewVBox(
		widget.NewCard("单只实时行情", "", container.NewVBox(g.codeEntry, rtBtn)),
		widget.NewCard("批量实时行情", "", container.NewVBox(g.multiEntry, batchBtn)),
		widget.NewCard("结果", "", container.NewScroll(g.resultBox)),
		clearBtn,
	)
}

// -------- Tab 3: 日志 --------
func (g *guiApp) buildLogTab() *fyne.Container {
	g.logView = widget.NewMultiLineEntry()
	g.logView.SetPlaceHolder("（启动服务器后将显示运行日志...）")
	g.logView.Wrapping = fyne.TextWrapWord
	copyBtn := widget.NewButtonWithIcon("复制全部", theme.ContentCopyIcon(), func() {
		g.logMu.Lock()
		txt := g.logBuf.String()
		g.logMu.Unlock()
		g.app.Clipboard().SetContent(txt)
		g.appendLog("📋 日志已复制到剪贴板")
	})
	clearBtn := widget.NewButton("清空", func() {
		g.logMu.Lock()
		defer g.logMu.Unlock()
		g.logBuf.Reset()
		g.logView.SetText("")
	})
	return container.NewBorder(
		widget.NewLabel("服务器 & GUI 运行日志（最多保留 200KB）"),
		container.NewGridWithColumns(2, copyBtn, clearBtn),
		nil, nil,
		container.NewScroll(g.logView),
	)
}

// -------- start/stop --------
// startServer 将启动过程放到 goroutine 中执行。
// 原因：core.StartServer 内部有网络操作（mDNS 绑定、IP 探测等），
// 若直接在 GUI 事件线程同步调用会导致界面卡死；
// 且 startServer 持有 g.mu 时调用 running.Set(true) 会触发监听器
// 再次请求 g.mu，形成死锁。
func (g *guiApp) startServer() {
	// 先做参数解析（在 GUI 线程，快）
	port := parseInt(g.portEntry.Text, 8080, 1024, 65535)
	syncInterval := parseInt(g.syncEntry.Text, 60, 0, 86400)
	g.cfg.Port = port
	g.cfg.SyncInterval = syncInterval

	// 快速检查是否已在运行 / 正在启动（不持锁等待）
	g.mu.Lock()
	if g.server != nil || g.starting {
		g.mu.Unlock()
		return
	}
	g.starting = true
	g.mu.Unlock()

	g.appendLog(fmt.Sprintf("▶ 启动：端口 %d | TDX=%v | MQTT=%v | DEBUG=%v",
		port, g.cfg.UseTDX, g.cfg.EnableMQTT, g.cfg.Debug))
	g.appendLog(fmt.Sprintf("📦 当前版本: v%s", g.version()))

	go func() {
		// panic 恢复：任何启动阶段的意外崩溃都不能拖垮 GUI
		defer func() {
			if r := recover(); r != nil {
				g.appendLog(fmt.Sprintf("❌ 启动异常（已恢复）: %v", r))
				g.mu.Lock()
				g.starting = false
				g.mu.Unlock()
				_ = g.running.Set(false)
			}
		}()

		cfg := core.ServerConfig{
			Host:         "0.0.0.0",
			Port:         port,
			SyncInterval: syncInterval,
			UseTDX:       g.cfg.UseTDX,
			EnableMQTT:   g.cfg.EnableMQTT,
			Debug:        g.cfg.Debug,
			DBPath:       "",
		}

		// 超时保护：30 秒安全网（discovery.Start 已改为异步，
		// 正常情况下 StartServer 会在 1-2 秒内返回）
		type result struct {
			rs  *core.RunningServer
			err error
		}
		ch := make(chan result, 1)
		go func() {
			rs, err := core.StartServer(cfg, false)
			ch <- result{rs, err}
		}()

		// 启动进度心跳：每 3 秒输出一条日志，让用户知道 GUI 仍在等待
		// （正常启动 <2s，若看到心跳说明有步骤卡住）
		heartbeat := time.NewTicker(3 * time.Second)
		defer heartbeat.Stop()

		var rs *core.RunningServer
		var err error
	waitLoop:
		for {
			select {
			case res := <-ch:
				rs, err = res.rs, res.err
				break waitLoop
			case <-heartbeat.C:
				g.appendLog("⏳ 启动中... 若持续无响应请查看「日志」Tab 的 step 输出")
			case <-time.After(30 * time.Second):
				err = fmt.Errorf("启动超时（30s）。请检查：1) 日志 Tab 的 step 进度定位卡在哪一步；2) 网络/TDX/SQLite 配置；3) 是否安装了最新版本（旧版存在启动阻塞 bug）")
				break waitLoop
			}
		}

		if err != nil {
			g.appendLog("❌ 启动失败：" + err.Error())
			g.mu.Lock()
			g.starting = false
			g.pendingDialog = err.Error()
			g.mu.Unlock()
			_ = g.running.Set(false)
			return
		}

		// 先存 server，再设 running 状态。
		// 关键：不能在持有 g.mu 时调用 running.Set，否则监听器会
		// 回调 refreshMDNSStatus 再次请求 g.mu → 死锁。
		g.mu.Lock()
		g.server = rs
		g.starting = false
		g.mu.Unlock()

		// 验证 HTTP 服务确实在监听（StartServer 把 ListenAndServe 放在
		// goroutine 里，端口被占用等错误只打日志不返回，需要主动探测）。
		healthURL := fmt.Sprintf("http://127.0.0.1:%d/api/health", port)
		alive := false
		for i := 0; i < 20; i++ { // 最多等 2 秒
			hc := &http.Client{Timeout: 500 * time.Millisecond}
			if resp, err := hc.Get(healthURL); err == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					alive = true
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !alive {
			g.appendLog("❌ 服务器进程已启动但 HTTP 端口无响应（可能端口被占用）")
			_ = rs.Stop()
			g.mu.Lock()
			g.server = nil
			g.pendingDialog = fmt.Sprintf("HTTP 端口 %d 无响应，可能已被占用", port)
			g.mu.Unlock()
			_ = g.running.Set(false)
			return
		}

		_ = g.running.Set(true)
		g.appendLog("✅ 服务器监听 " + rs.Addr)
	}()
}

func (g *guiApp) stopServer() {
	g.mu.Lock()
	if g.server == nil {
		g.mu.Unlock()
		return
	}
	rs := g.server
	g.server = nil
	// 先释放锁再调用 running.Set，避免监听器回调死锁
	g.mu.Unlock()

	g.appendLog("■ 请求停止服务器...")
	_ = g.running.Set(false)
	if err := rs.Stop(); err != nil {
		g.appendLog("⚠️ " + err.Error())
	} else {
		g.appendLog("✅ 已停止")
	}
}

// -------- helpers --------
// version 优先用 Fyne 打包时注入的 app 元数据（-appVersion），
// 开发模式（go run）下回退到 core.Version()（ldflags 注入或默认值）
func (g *guiApp) version() string {
	if g.app != nil {
		if m := g.app.Metadata(); m.Version != "" && m.Version != "0.0" {
			if m.Build > 0 {
				return fmt.Sprintf("%s (build %d)", m.Version, m.Build)
			}
			return m.Version
		}
	}
	return core.Version()
}

func (g *guiApp) appendLog(line string) {
	full := fmt.Sprintf("[%s] %s\n", time.Now().Format("15:04:05"), line)
	g.logMu.Lock()
	g.logBuf.WriteString(full)
	if g.logBuf.Len() > 200*1024 {
		s := g.logBuf.String()
		g.logBuf.Reset()
		g.logBuf.WriteString(s[len(s)/2:])
	}
	g.logMu.Unlock()
	// 不在此处直接更新 UI！
	// Android 上 fyne.App 不实现 QueueUpdate，从 goroutine 调用 SetText
	// 会与 GL 渲染线程死锁，导致 log 包互斥锁无法释放 → 后续所有
	// log.Println 调用永久阻塞 → 启动超时 30s。
	// 改为：动画定时器在 UI 线程上周期性读取 logBuf 并刷新 logView。
}

func parseInt(s string, def, lo, hi int) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return def
	}
	switch {
	case n < lo:
		return lo
	case n > hi:
		return hi
	}
	return n
}

// -------- 行情渲染（严格对应 core.RealtimeData / core.QuoteRecord 字段）--------
func renderOne(r *core.RealtimeQuote) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s (%s)\n", r.Name, r.Code)
	fmt.Fprintf(&b, "  当前价 %7.3f      昨收 %7.3f      今开 %7.3f\n", r.Price, r.LastClose, r.Open)
	fmt.Fprintf(&b, "  最高  %7.3f      最低 %7.3f\n", r.High, r.Low)
	chg := r.ChangeAmt
	pct := r.ChangePct
	sign := ""
	if chg > 0 {
		sign = "+"
	}
	fmt.Fprintf(&b, "  涨跌 %s%7.3f  (%s%6.2f%%)\n", sign, chg, sign, pct)
	fmt.Fprintf(&b, "  成交量 %8.0f 手      成交额 %.0f 元\n", r.Volume/100, r.Amount)
	fmt.Fprintf(&b, "  数据源：%s", "腾讯 HTTP / 缓存（core.RealtimeData）")
	return b.String()
}

func renderMany(m map[string]*core.BatchQuoteRecord) string {
	if len(m) == 0 {
		return "空结果"
	}
	type row struct {
		k string
		r *core.BatchQuoteRecord
	}
	rows := make([]row, 0, len(m))
	for k, v := range m {
		if v != nil {
			rows = append(rows, row{k, v})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].k < rows[j].k })
	var b strings.Builder
	fmt.Fprintf(&b, "%-8s %-10s %10s %10s %10s %12s\n",
		"代码", "名称", "现价", "涨跌额", "涨跌幅%", "成交量(手)")
	fmt.Fprintf(&b, strings.Repeat("-", 64)+"\n")
	for _, rr := range rows {
		r := rr.r
		last := r.PrevClose
		chg := r.Price - last
		pct := 0.0
		if last > 0 {
			pct = chg / last * 100
		}
		sign := ""
		if chg > 0 {
			sign = "+"
		}
		fmt.Fprintf(&b, "%-8s %-10s %10.3f %s%9.3f %s%8.2f%% %12.0f\n",
			r.Code, truncate(r.Name, 8), r.Price, sign, chg, sign, pct, r.Volume/100)
		if r.Stale {
			b.WriteString("     ⚠️ stale\n")
		}
	}
	return b.String()
}
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
