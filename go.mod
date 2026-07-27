module go-stock-server

go 1.26

require (
	github.com/eclipse/paho.mqtt.golang v1.5.0
	github.com/miekg/dns v1.1.41
	github.com/weijia/go-tdx v0.1.0
	golang.org/x/sys v0.22.0
	golang.org/x/text v0.16.0
)

require (
	github.com/gorilla/websocket v1.5.3 // indirect
	golang.org/x/net v0.27.0 // indirect
	golang.org/x/sync v0.7.0 // indirect
)

replace github.com/weijia/go-tdx => ../go-tdx
