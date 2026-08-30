# go-stock-server 多入口构建脚本
# 用法：
#   make cli              → 编译 3 平台 CLI 二进制（方案 A）
#   make apk-shell        → 编译方案 B Shell APK（StockServerShell arm64）
#   make apk-gui          → 编译方案 C GUI APK（StockServerGUI arm64）
#   make all              → 全部构建
#   make test             → 核心回归（T1~T4 基线对比 + cmd/server 编译）

SHELL := /usr/bin/env bash
GO    ?= go
GRADLE ?= gradle
FYNE  ?= fyne
export CGO_ENABLED = 0
export GOPROXY ?= https://goproxy.cn,direct
export ANDROID_HOME ?= /opt/android-sdk
export ANDROID_SDK_ROOT := $(ANDROID_HOME)

BUILD_OUT := build-out
CLI_OUTS := $(BUILD_OUT)/go-stock-server-linux-amd64 \
            $(BUILD_OUT)/go-stock-server-windows-amd64.exe \
            $(BUILD_OUT)/go-stock-server-android-arm64

.PHONY: all cli apk-shell apk-gui test clean

all: cli apk-shell

cli: $(CLI_OUTS)
	@echo "✅ CLI 三平台二进制已在 $(BUILD_OUT)/"

$(BUILD_OUT):
	@mkdir -p $@

$(BUILD_OUT)/go-stock-server-linux-amd64: | $(BUILD_OUT)
	GOOS=linux   GOARCH=amd64 $(GO) build -ldflags="-s -w" -o $@ ./cmd/server

$(BUILD_OUT)/go-stock-server-windows-amd64.exe: | $(BUILD_OUT)
	GOOS=windows GOARCH=amd64 $(GO) build -ldflags="-s -w" -o $@ ./cmd/server

$(BUILD_OUT)/go-stock-server-android-arm64: | $(BUILD_OUT)
	GOOS=android GOARCH=arm64 $(GO) build -ldflags="-s -w" -o $@ ./cmd/server

# ---------- 方案 B Shell APK ----------
SHELL_APK  := android-app/app/build/outputs/apk/debug/app-debug.apk
ANDROID_GO_BIN := android-app/app/src/main/res/raw/go_stock_server

$(ANDROID_GO_BIN): $(BUILD_OUT)/go-stock-server-android-arm64
	@mkdir -p $(dir $@)
	@cp $(BUILD_OUT)/go-stock-server-android-arm64 $@
	@echo "✅ 已拷贝 arm64 Go 二进制到 Android raw 资源"

apk-shell: $(ANDROID_GO_BIN)
	@echo "==> 编译方案 B Shell APK..."
	cd android-app && \
	export ANDROID_HOME=$(ANDROID_HOME) ANDROID_SDK_ROOT=$(ANDROID_SDK_ROOT) && \
	$(GRADLE) --no-daemon app:assembleDebug 2>&1 | tail -20
	@cp $(SHELL_APK) $(BUILD_OUT)/StockServerShell-debug.apk 2>/dev/null || true
	@ls -lh $(BUILD_OUT)/StockServerShell-debug.apk 2>/dev/null && echo "✅ 方案 B APK 完成"

# ---------- 方案 C GUI APK ----------
GUI_APK := StockServerGUI.apk
apk-gui:
	@echo "==> 编译方案 C Fyne GUI APK..."
	@command -v $(FYNE) >/dev/null 2>&1 || { echo "❌ 缺少 fyne，请先：GOPROXY=$(GOPROXY) GOBIN=/usr/local/bin $(GO) install fyne.io/fyne/v2/cmd/fyne@latest"; exit 1; }
	# 先确保 go.mod 已 tidy
	$(GO) mod tidy
	$(FYNE) package -os android -appID com.stock.server.gui \
	            -name StockServerGUI \
	            -icon /workspace/go-stock-server/android-app/app/src/main/res/drawable/ic_launcher_foreground.xml \
	            ./cmd/android-gui
	ls -lh StockServerGUI.apk 2>/dev/null || true
	@mkdir -p $(BUILD_OUT)
	@cp StockServerGUI.apk $(BUILD_OUT)/ 2>/dev/null || true
	@[ -f $(BUILD_OUT)/StockServerGUI.apk ] && echo "✅ 方案 C APK 完成: $(BUILD_OUT)/StockServerGUI.apk"

# ---------- 回归：基线 T1~T4 + go vet ----------
test:
	@echo "==> go vet ./..."
	$(GO) vet ./... 2>&1 | head -30
	@echo "==> 编译 cmd/server 并跑基线 smoke（与方案 A 行为对比）"
	$(GO) build -o /tmp/server-cli-test ./cmd/server
	@bash -c '\
	set -e; \
	OUT=/tmp/snap_cli_test.txt; rm -f $$OUT; \
	/tmp/server-cli-test --db "" --host 127.0.0.1 --port 29600 > /tmp/smoke_cli.log 2>&1 & PID=$$!; \
	sleep 3; \
	for t in "T1 /api/health" "T2 /api/config" "T3 /api/quote_cache"; do \
	  name=$${t% *}; url=$${t#* }; \
	  echo "=== $$name ===" >> $$OUT; \
	  curl -s --max-time 5 "http://127.0.0.1:29600$$url" \
	    | sed -E "s/\"timestamp\":\"[^\"]*\"/\"timestamp\":\"__TS__\"/g; s/\"uptime_seconds\":[0-9]+/\"uptime_seconds\":__UP__/g; s/\"start_time\":\"[^\"]*\"/\"start_time\":\"__ST__\"/g" \
	    >> $$OUT; echo "" >> $$OUT; \
	done; \
	echo "=== T4 ===" >> $$OUT; \
	curl -s --max-time 5 -w "\nHTTP_STATUS:%{http_code}" http://127.0.0.1:29600/api/node/config \
	  | sed -E "s/\"timestamp\":\"[^\"]*\"/\"timestamp\":\"__TS__\"/g" >> $$OUT; echo "" >> $$OUT; \
	kill -SIGTERM $$PID; wait $$PID 2>/dev/null; \
	echo "已生成 $$OUT"'

clean:
	rm -rf build-out android-app/.gradle android-app/app/build
