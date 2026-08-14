module go-stock-server

go 1.26

require (
	github.com/eclipse/paho.mqtt.golang v1.5.0
	github.com/miekg/dns v1.1.41
	github.com/weijia/go-tdx v0.1.0
	golang.org/x/sys v0.47.0
	golang.org/x/text v0.16.0
	modernc.org/sqlite v1.56.0
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/net v0.27.0 // indirect
	golang.org/x/sync v0.21.0 // indirect
	modernc.org/libc v1.74.4 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace github.com/weijia/go-tdx => ../go-tdx
