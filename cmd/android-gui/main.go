// 股票行情服务器方案 C GUI 主入口（跨平台桌面 + Android）
// 桌面用法（需 CGO OpenGL）:
//   go run ./cmd/android-gui
// Android APK:
//   fyne package -os android -appID com.stock.server.gui -name StockServerGUI -icon <PNG> ./cmd/android-gui
//   （若 fyne 在子目录找 main 失败，可把本文件复制到 module 根目录后用「.」做目标）

package main

import (
	"fmt"
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

	server *core.RunningServer
	mu     sync.Mutex

	portEntry *widget.Entry
	syncEntry *widget.Entry
	swTdx     *widget.Check
	swMqtt    *widget.Check
	swDebug   *widget.Check
	statusLbl *widget.Label
	ipLbl     *widget.Label
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

func main() {
	g := &guiApp{
		app:     app.NewWithID("com.stock.server.gui"),
		cfg:     core.DefaultConfig(),
		running: binding.NewBool(),
	}
	g.app.Settings().SetTheme(theme.DefaultTheme())
	g.win = g.app.NewWindow("📈 股票行情服务器 v" + core.Version())
	g.win.Resize(fyne.NewSize(420, 720))
	g.win.SetMaster()

	g.fetcher = core.NewStockFetcher(false)
	g.buildTabs()
	g.running.AddListener(binding.NewDataListener(func() {
		r, _ := g.running.Get()
		g.refreshRunState(r)
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
	} else {
		g.statusLbl.SetText("⏸ 已停止")
		g.statusLbl.Importance = widget.DangerImportance
		g.ipLbl.SetText("")
	}
	if r { g.startBtn.Disable(); g.stopBtn.Enable()
	} else { g.startBtn.Enable();  g.stopBtn.Disable() }
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
	g.swTdx   = widget.NewCheck("启用通达信 TCP 数据源", func(c bool) { g.cfg.UseTDX = c })
	g.swMqtt  = widget.NewCheck("启用 MQTT 接入层",      func(c bool) { g.cfg.EnableMQTT = c })
	g.swDebug = widget.NewCheck("DEBUG 详细日志",          func(c bool) { g.cfg.Debug = c })
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
	g.startBtn = widget.NewButtonWithIcon("▶ 启动服务器", theme.MediaPlayIcon(), g.startServer)
	g.startBtn.Importance = widget.HighImportance
	g.stopBtn  = widget.NewButtonWithIcon("■ 停止", theme.MediaStopIcon(), g.stopServer)
	g.stopBtn.Importance = widget.DangerImportance
	urlCard := widget.NewCard("浏览器 / 客户端访问", "", container.NewVBox(
		widget.NewLabel("同 WiFi 下访问："),
		widget.NewLabel("健康检查：  http://<本机IP>:端口/api/health"),
		widget.NewLabel("示例(茅台)：http://<本机IP>:端口/api/realtime/600519"),
		widget.NewLabel("批量：         /api/batch/quotes?codes=000001,601318,600519"),
	))
	return container.NewVBox(
		widget.NewCard("运行状态", "", container.NewVBox(g.statusLbl, g.ipLbl)),
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
		if c == "" { dialog.ShowInformation("提示", "请输入股票代码", g.win); return }
		g.appendLog(fmt.Sprintf("FetchRealtime(%s)", c))
		r, err := g.fetcher.FetchRealtime(c)
		if err != nil { g.resultBox.SetText("❌ " + err.Error()); return }
		if r == nil { g.resultBox.SetText("❌ 结果为空"); return }
		g.resultBox.SetText(renderOne(r))
	})
	batchBtn := widget.NewButton("▶ 批量查询", func() {
		raw := strings.Split(strings.ReplaceAll(g.multiEntry.Text, " ", ""), ",")
		cs := raw[:0]
		for _, s := range raw { if s != "" { cs = append(cs, s) } }
		if len(cs) == 0 { dialog.ShowInformation("提示", "请输入至少 1 个代码", g.win); return }
		g.appendLog(fmt.Sprintf("FetchBatchQuotes(%d codes)", len(cs)))
		m, err := g.fetcher.FetchBatchQuotes(cs)
		if err != nil { g.resultBox.SetText("❌ " + err.Error()); return }
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
	return container.NewBorder(
		widget.NewLabel("服务器 & GUI 运行日志（最多保留 200KB）"),
		widget.NewButton("清空", func() {
			g.logMu.Lock(); defer g.logMu.Unlock()
			g.logBuf.Reset(); g.logView.SetText("")
		}), nil, nil,
		container.NewScroll(g.logView),
	)
}

// -------- start/stop --------
func (g *guiApp) startServer() {
	g.cfg.Port = parseInt(g.portEntry.Text, 8080, 1024, 65535)
	g.cfg.SyncInterval = parseInt(g.syncEntry.Text, 60, 0, 86400)
	g.cfg.Host = "0.0.0.0"
	g.cfg.DBPath = ""
	g.mu.Lock(); defer g.mu.Unlock()
	if g.server != nil { return }
	g.appendLog(fmt.Sprintf("▶ 启动：端口 %d | TDX=%v | MQTT=%v | DEBUG=%v",
		g.cfg.Port, g.cfg.UseTDX, g.cfg.EnableMQTT, g.cfg.Debug))
	rs, err := core.StartServer(core.ServerConfig{
		Host:         g.cfg.Host,
		Port:         g.cfg.Port,
		SyncInterval: g.cfg.SyncInterval,
		UseTDX:       g.cfg.UseTDX,
		EnableMQTT:   g.cfg.EnableMQTT,
		Debug:        g.cfg.Debug,
		DBPath:       g.cfg.DBPath,
	}, false)
	if err != nil {
		g.appendLog("❌ 启动失败：" + err.Error())
		dialog.ShowError(err, g.win); return
	}
	g.server = rs
	_ = g.running.Set(true)
	g.appendLog("✅ 服务器监听 " + rs.Addr)
}
func (g *guiApp) stopServer() {
	g.mu.Lock(); defer g.mu.Unlock()
	if g.server == nil { return }
	g.appendLog("■ 请求停止服务器...")
	if err := g.server.Stop(); err != nil { g.appendLog("⚠️ " + err.Error()) } else { g.appendLog("✅ 已停止") }
	g.server = nil
	_ = g.running.Set(false)
}

// -------- helpers --------
func (g *guiApp) appendLog(line string) {
	full := fmt.Sprintf("[%s] %s\n", time.Now().Format("15:04:05"), line)
	g.logMu.Lock()
	g.logBuf.WriteString(full)
	if g.logBuf.Len() > 200*1024 {
		s := g.logBuf.String(); g.logBuf.Reset(); g.logBuf.WriteString(s[len(s)/2:])
	}
	txt := g.logBuf.String()
	g.logMu.Unlock()
	// Fyne v2 标准：用 QueueUpdate（app 对象有 Queue 能力）避免跨线程刷新
	if q, ok := interface{}(g.app).(interface{ QueueUpdate(func()) }); ok {
		q.QueueUpdate(func() { g.logView.SetText(txt) })
	} else {
		g.logView.SetText(txt)
	}
}

func parseInt(s string, def, lo, hi int) int {
	s = strings.TrimSpace(s)
	if s == "" { return def }
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil { return def }
	switch {
	case n < lo: return lo
	case n > hi: return hi
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
	sign := ""; if chg > 0 { sign = "+" }
	fmt.Fprintf(&b, "  涨跌 %s%7.3f  (%s%6.2f%%)\n", sign, chg, sign, pct)
	fmt.Fprintf(&b, "  成交量 %8.0f 手      成交额 %.0f 元\n", r.Volume/100, r.Amount)
	fmt.Fprintf(&b, "  数据源：%s", "腾讯 HTTP / 缓存（core.RealtimeData）")
	return b.String()
}

func renderMany(m map[string]*core.BatchQuoteRecord) string {
	if len(m) == 0 { return "空结果" }
	type row struct{ k string; r *core.BatchQuoteRecord }
	rows := make([]row, 0, len(m))
	for k, v := range m { if v != nil { rows = append(rows, row{k, v}) } }
	sort.Slice(rows, func(i, j int) bool { return rows[i].k < rows[j].k })
	var b strings.Builder
	fmt.Fprintf(&b, "%-8s %-10s %10s %10s %10s %12s\n",
		"代码", "名称", "现价", "涨跌额", "涨跌幅%", "成交量(手)")
	fmt.Fprintf(&b, strings.Repeat("-", 64)+"\n")
	for _, rr := range rows {
		r := rr.r
		last := r.PrevClose
		chg := r.Price - last
		pct := 0.0; if last > 0 { pct = chg / last * 100 }
		sign := ""; if chg > 0 { sign = "+" }
		fmt.Fprintf(&b, "%-8s %-10s %10.3f %s%9.3f %s%8.2f%% %12.0f\n",
			r.Code, truncate(r.Name, 8), r.Price, sign, chg, sign, pct, r.Volume/100)
		if r.Stale { b.WriteString("     ⚠️ stale\n") }
	}
	return b.String()
}
func truncate(s string, n int) string {
	r := []rune(s); if len(r) <= n { return s }; return string(r[:n]) + "…" }
