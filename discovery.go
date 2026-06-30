// go-stock-server/discovery.go - 服务发现（mDNS 响应/宣告 + UDP 广播）
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const (
	discoveryPort     = 8081
	discoveryInterval = 10 * time.Second
	mdnsServiceType   = "_stock-server._tcp"
	mdnsPort          = 5353
	mdnsMulticastIPv4 = "224.0.0.251"
	mdnsAnnounceSecs  = 60 // 主动宣告间隔（秒）
)

// ServiceDiscovery 服务发现
type ServiceDiscovery struct {
	httpPort     int
	instanceName string
	localIP      net.IP
	fqdn         string

	mdnsConn    *net.UDPConn
	mdnsStop    chan struct{}
	mdnsOK      bool
	queryCnt    int // 收到的查询数

	udpConn      *net.UDPConn
	udpStop      chan struct{}
	broadcastCnt int

	wg sync.WaitGroup
}

// NewServiceDiscovery 创建服务发现实例
func NewServiceDiscovery(httpPort int) *ServiceDiscovery {
	hostname, _ := os.Hostname()
	return &ServiceDiscovery{
		httpPort:     httpPort,
		instanceName: hostname,
		mdnsStop:     make(chan struct{}),
		udpStop:      make(chan struct{}),
	}
}

// Start 启动服务发现
func (d *ServiceDiscovery) Start() {
	d.localIP = d.findLocalIPv4()
	if d.localIP == nil {
		log.Println("[服务发现] 无法获取本机 IPv4，仅启动 UDP 广播")
		go d.udpBroadcastLoop()
		return
	}
	d.fqdn = fmt.Sprintf("%s.%s.local", d.instanceName, mdnsServiceType)

	d.startMDNS()
	go d.udpBroadcastLoop()
}

// Stop 停止服务发现
func (d *ServiceDiscovery) Stop() {
	d.stopMDNS()
	if d.udpConn != nil {
		close(d.udpStop)
		d.udpConn.Close()
		log.Printf("[服务发现-UDP] UDP 广播停止，共发送 %d 次广播", d.broadcastCnt)
	}
	d.wg.Wait()
}

// findLocalIPv4 找本机主网卡 IPv4
func (d *ServiceDiscovery) findLocalIPv4() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err == nil {
		ip := conn.LocalAddr().(*net.UDPAddr).IP
		conn.Close()
		if ip4 := ip.To4(); ip4 != nil && !ip4.IsLoopback() {
			return ip4
		}
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			if ipNet, ok := addr.(*net.IPNet); ok {
				if ip4 := ipNet.IP.To4(); ip4 != nil && !ip4.IsLoopback() {
					return ip4
				}
			}
		}
	}
	return nil
}

// udpBroadcastLoop UDP 广播循环
func (d *ServiceDiscovery) udpBroadcastLoop() {
	d.wg.Add(1)
	defer d.wg.Done()

	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{
		IP:   net.IPv4(255, 255, 255, 255),
		Port: discoveryPort,
	})
	if err != nil {
		log.Printf("[服务发现-UDP] 创建广播 socket 失败: %v", err)
		return
	}
	d.udpConn = conn

	ticker := time.NewTicker(discoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.sendUDPBroadcast()
		case <-d.udpStop:
			return
		}
	}
}

func (d *ServiceDiscovery) sendUDPBroadcast() {
	ip := d.localIP.String()
	msg := map[string]interface{}{
		"service":   "stock-server",
		"instance":  d.instanceName,
		"http_port": d.httpPort,
		"address":   ip,
		"timestamp": time.Now().Unix(),
	}
	data, _ := json.Marshal(msg)
	addr := &net.UDPAddr{IP: net.IPv4(255, 255, 255, 255), Port: discoveryPort}
	d.udpConn.WriteToUDP(data, addr)
	d.broadcastCnt++
	log.Printf("[服务发现-UDP] 广播 #%d → 255.255.255.255:%d", d.broadcastCnt, discoveryPort)
}

// PrintInfo 打印服务发现信息
func (d *ServiceDiscovery) PrintInfo(localIP string) {
	if d.mdnsOK {
		log.Printf("服务发现: mDNS (%s)", d.fqdn)
		log.Println("  Android NSD / iOS Bonjour 搜索: _stock-server._tcp")
		log.Printf("  主动宣告: 每 %d 秒发送一次（mDNS 规范）", mdnsAnnounceSecs)
	} else {
		log.Printf("服务发现: UDP 广播 %d", discoveryPort)
	}
}

// getLocalIP 获取本机 IP
func getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}
