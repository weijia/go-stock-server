# go-stock-server：MQTT 接入层与行情数据源优先级

记录 my-quant 通过 MQTT 查价这条链路上的常见问题与结论。行号随代码变动可能漂移，以函数名为准。

## 相关文件

| 文件 | 职责 |
|---|---|
| `internal/core/mqtt.go` | MQTT 接入层：连接、订阅、命令执行、`quote_push` 推送 |
| `internal/core/mqtt_crypto.go` | OpenSSL 兼容 AES 加解密（与 CryptoJS.AES 互通） |
| `internal/core/handlers.go` | HTTP handler 与 `fetchBatchQuotes`（数据源合并） |
| `internal/core/fetcher.go` | 腾讯 / 新浪 / 东财 HTTP 数据源 |
| `internal/core/tdx_source.go` | 通达信 TCP 数据源 |
| `internal/core/main.go` | `RunLegacyMain` / `ServerConfig` / `DefaultConfig`（实际入口） |
| `cmd/server/main.go` | CLI 入口，转调 `core.RunLegacyMain` |

构建入口是 `./cmd/server`（见 `Makefile` 的 `cli` 目标）；仓库根目录的 `main.go` 是遗留文件，缺少 `go-stock-server/internal/core` 的 import，当前无法编译。

---

## Q1：go-stock-server 实现了 my-quant 用的 MQTT 查价接口吗？

实现了。`internal/core/mqtt.go` + `mqtt_crypto.go` 就是 Price Server 侧的 MQTT 接入层，`my-quant/src/services/MQTTPriceService.js` 是它的客户端，两端协议对齐：

| 项 | my-quant | go-stock-server |
|---|---|---|
| broker | `wss://broker.emqx.io:8084/mqtt` | `MQTT_BROKER` 默认值相同 |
| topic | `secure/stock-price-mqtt` | `MQTT_TOPIC` 默认值相同 |
| 密码 | `stock_price_mqtt_S3cret` | `MQTT_PASSWORD` 默认值相同 |
| 加密 | CryptoJS.AES | `Salted__` + EVP_BytesToKey(MD5) + AES-256-CBC，与 CryptoJS 互通 |
| 信封 | `{id, msgId, user, msg(JSON 字符串), time}` | `MQTTPriceClient.publish()` 同结构 |
| 订阅 | `{action:'subscribe', data:{codes, interval}}` | `handleSubscription()`：subscribe / unsubscribe / list_subs |
| 报价 | 只认 `{action:'quote_push', data:{code,name,price,open,prev_close,change_pct,price_ts,stale}}` | `pushLoop()` 按 `pushInterval` 逐只推送，字段一致 |
| 自消息过滤 | `envelope.id === clientId` 时忽略 | `onMessage()` 中 `senderID == c.clientID` 直接 return |

服务端另有一批 my-quant 当前未使用的请求-响应式命令：`realtime`、`batch`、`kline`、`qfq`、`minute`、`intraday`、`name`、`quote_cache`、`health`、`config`，响应包成 `{action:'response', data:{...}}`。my-quant 只解析 `quote_push`，这些 `response` 会被静默丢弃。

注意 my-quant 的行情是**单向推送**模式：发一次 `subscribe`，之后被动接收 `quote_push`，没有"主动查一次"的通道。要主动查询需在 my-quant 侧补 `response` 解析，或改发 `realtime` / `batch` 命令。

## Q2：连到哪个 MQTT 服务器？

默认连 EMQX 公共集群 `wss://broker.emqx.io:8084/mqtt`（WebSocket + TLS，端口 8084，路径 `/mqtt`），不是自建 broker。my-quant 的 `priceMqtt.serverUrl`（`app-config/my-quant/config.json`）是同一地址同一 topic，两端靠约定的 AES 密码做隔离——**无 broker 账号鉴权**，消息体加密即唯一屏障。

覆盖方式（优先级：命令行 > 环境变量 > 默认值）：

```
go-stock-server -mqtt-broker wss://xxx:8084/mqtt -mqtt-topic <topic> -mqtt-password <pwd>
MQTT_BROKER=... MQTT_TOPIC=... MQTT_PASSWORD=... go-stock-server
```

`parseBrokerURL()` 支持 `mqtt://`(1883)、`mqtts/ssl/tls`(8883)、`ws/wss`(8084)，并按 scheme 决定是否启用 WebSocket 与 TLS。

私有化部署时，`MQTT_BROKER`、`MQTT_TOPIC`、`MQTT_PASSWORD` 三者应同时更换，并同步更新 my-quant 的 `priceMqtt` 配置。

## Q3：MQTT / TDX 默认是开还是关？

**当前默认均为开启**（此前 MQTT 默认关闭）：

| 开关 | 默认值 | 位置 |
|---|---|---|
| `MQTT_ENABLED` / `-mqtt` | `true` | `main.go:38`、`internal/core/main.go:105` |
| `TDX_ENABLED` / `-tdx` | `true` | `main.go:32`、`internal/core/main.go:100` |
| `DefaultConfig().EnableMQTT` | `true` | `internal/core/main.go:47`（决定 GUI 默认勾选） |
| `DefaultConfig().UseTDX` | `true` | `internal/core/main.go` 的 `DefaultConfig()` |

关闭：`-mqtt=false` / `MQTT_ENABLED=false`，`-tdx=false` / `TDX_ENABLED=false`。

`startFromConfig()` 内有参数兜底：`EnableMQTT` 为真而 `MQTTBroker` / `MQTTTopic` / `MQTTPassword` 为空时填入默认值，`MQTTPushInterval < 1` 时取 5；`UseTDX` 为真而 `TDXPort == 0` 时取 7709。这是必要的——GUI（`gui_main.go`、`cmd/android-gui/main.go`）启动服务器时只传 7 个字段，不含 MQTT 连接参数。

**副作用**：TDX 自动测速在 `step 2/8` 同步执行，HTTP 端口要等它结束才监听。连通达信的网络里很快；不通时固定约 5 秒超时后回退腾讯 HTTP。

## Q4：腾讯是 TDX 的兜底吗？

腾讯 HTTP 既是基础源也是兜底，TDX 是可选增强覆盖层，不是"TDX 为主、腾讯兜底"的单链。

| 能力 | 优先级 |
|---|---|
| 单只实时行情 | TDX → 腾讯 HTTP |
| 批量报价 / `quote_push` | 腾讯 HTTP 先取 → TDX 覆盖价格 |
| K线 / 前复权 | 纯 HTTP 多源：腾讯 → 新浪 → 东财，与 TDX 无关 |
| 分钟 / 分时 | TDX → 腾讯 → 东财（`fetcher.FetchMinute`） |

`fetchBatchQuotes` 明确认定无 TDX 时腾讯价即实时价：

```go
// 无 TDX 时，腾讯价即实时价（非 close 兜底）
if h.tdx == nil || !h.tdx.Enabled() {
    for _, rec := range ten {
        if rec != nil {
            rec.PriceSource = ""
        }
    }
}
```

## Q5：到底先取 TDX 的实时价还是腾讯的？

两条路径不同：

1. **单只实时行情**（`/api/realtime/`、MQTT `realtime`）：**TDX 先试，失败才腾讯**。`HandleRealtime` 中先 `tdx.GetRealtime`，`data == nil || err != nil` 时回退 `fetcher.FetchRealtime`。MQTT 的 `realtime` 命令顺序相同。
2. **批量行情**（`/api/batch/quotes`、MQTT `batch`、`quote_push`）：**腾讯先取，TDX 后覆盖**。原因是腾讯批量接口一次性给出名称 / 昨收 / 开盘等字段，TDX 批量接口不给全，所以必须先请求腾讯；但最终生效的价格以 TDX 为准（`q.Price > 0` 覆盖实时价，否则用 TDX 昨收并标 `last_close`，再不行 `continue` 交给腾讯兜底）。

两处的**语义都是 TDX 优先**，只是批量路径因字段补齐需要，实现上先请求腾讯。

## Q6：my-quant 发了 subscribe，服务端回「缺少 codes」？

现象（my-quant 发出的报文，明文展示）：

```json
{"action":"subscribe","data":{"codes":["600519", ...],"interval":5}}
```

服务端回：

```json
{"action":"response","data":{"action":"subscribe","message":"缺少 codes","status":"error"}}
```

**根因**：命令负载约定放在内层 `data`（Python 版 `mqtt_protocol.py:600` 为 `data = inner.get("data") or {}`，my-quant 也按此发送）。Go 版在 `onMessage` 里却把整个 `inner`（即 `{action, data}`）传给了 `handleSubscription`，于是 `data["codes"]` 恒为空。

**已修复**：`onMessage` 中先取出内层 `data` 再分发，订阅与行情命令共用同一个 `data`：

```go
	// 命令负载：约定放在内层 data（与 Python mqtt_protocol 及 my-quant 一致）；
	// 兼容早期扁平写法（codes / interval 与 action 同级）。
	data := map[string]interface{}{}
	if d, ok := inner["data"].(map[string]interface{}); ok {
		data = d
	}
	if _, ok := data["codes"]; !ok {
		if raw, ok := inner["codes"]; ok {
			data["codes"] = raw
		}
	}
	if _, ok := data["interval"]; !ok {
		if raw, ok := inner["interval"]; ok {
			data["interval"] = raw
		}
	}
```

兼容扁平写法的分支是为设计文档时序图里的 `加密{action:"subscribe",codes:"000001,600000"}` 这类老报文保留的，新客户端统一用嵌套 `data`。

---

## 已修复的问题

以下三处均已在本轮修复并验证（`go test` 冒烟：发 subscribe → 收到 `status=success` 的 response → 收到 `quote_push`）：

| 问题 | 修复位置 |
|---|---|
| subscribe 报「缺少 codes」（见 Q6） | `internal/core/mqtt.go` 的 `onMessage` |
| TDX 不可用时 `quote_push` 不发 | `internal/core/mqtt.go` 的 `pushLoop`：移除 `tdx == nil \|\| !tdx.Enabled()` 门禁，改为腾讯 HTTP 兜底即可推送 |
| my-quant 已连接后不再发 subscribe（codes 变化不生效） | `my-quant/src/services/MQTTPriceService.js`：`subscribeQuotes()` 补发一次，`publishSubscribe()` 按 codes 去重，`close` 时清空去重键以便重连后重发 |

推送侧同时补了日志：订阅命令进 `log.Printf`（含 codes / interval / msgId），无行情可推时 `log.Printf`，推送明细走 `--debug`。

**验证结果**（服务器 `-tdx=false`，即纯腾讯 HTTP 兜底）：

```
收到 action=response body={"action":"response","data":{"action":"subscribe","data":{"count":1,"push_interval":5,"subscribed":["600519"]},"status":"success"}}
收到 action=quote_push body={"action":"quote_push","data":{"change_pct":2.0972,"code":"600519","name":"贵州茅台","price":1312.97,"prev_close":1286,"stale":false,...}}
```

## 已知问题（未修复）

### 1. MQTT 的 `minute` / `intraday` 命令没有 HTTP 兜底

这两个命令只走 `tdx.GetMinute`，失败即返回「无可用分钟数据源」，未接上 `fetcher.FetchMinute` 现成的腾讯 / 东财兜底。

### 2. 既有的编译问题（与 MQTT 无关）

- `cmd/android-gui`：依赖 fyne + OpenGL，需 CGO，Windows 本机缺 CGO 环境时构建失败。
- 根目录 `main.go`：引用 `NewStockFetcher`、`TdxDataSource`、`MQTTPriceClient` 等 core 包符号却未 import `go-stock-server/internal/core`，无法编译。实际入口请用 `./cmd/server`。
