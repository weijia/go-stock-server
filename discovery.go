// go-stock-server/discovery.go - 服务发现（mDNS 响应/宣告 + UDP 广播）
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
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
	// selfHealInterval 自检兜底周期：周期性重新探测主 IPv4，与当前不一致即重建 mDNS。
	// 作为各平台 IP 变化事件之外的保底——休眠/换网后若系统事件未及时触发（地址变更、
	// 早期 APIPA 阶段、或事件丢失），最多该周期后必然恢复。Linux/BSD 无 OS 地址变更
	// 事件，纯靠轮询，故设为 30s 以在换网后快速自愈。
	selfHealInterval = 30 * time.Second
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

	// ipNotifyCancel 取消 IP 变化监听（Windows 用 NotifyIpInterfaceChange /
	// NotifyUnicastIpAddressChange 注册，非 Windows 为 nil）。由 watchIPChange 设置，
	// Stop 时调用；watchIPChange 退出时也会自行取消。
	ipNotifyCancel func()

	// mu 保护 localIP / mDNS 重建，避免 selfHeal ticker 与 IP 事件回调并发重入。
	mu sync.Mutex
	// stopped 已停止标志，避免 Stop 之后仍触发重建。
	stopped bool
	// discoveryStop 整个发现生命周期的停止信号（watchIPChange / selfHealLoop 常驻）。
	discoveryStop chan struct{}

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

	d.discoveryStop = make(chan struct{})
	d.startMDNS()
	go d.udpBroadcastLoop()
	// 事件驱动监听本机 IP 变化（换网/拔插网卡后客户端仍能发现），IP 一变即重建 mDNS
	go d.watchIPChange()
	// 周期自检兜底：即使系统 IP 变化事件漏报，也能在 selfHealInterval 内自愈
	go d.selfHealLoop()
}

// onIPChanged 由平台 watchIPChange / selfHealLoop 在检测到 IP 变化时调用
func (d *ServiceDiscovery) onIPChanged(newIP net.IP) {
	if newIP == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return
	}
	if d.localIP != nil && d.localIP.Equal(newIP) {
		return
	}
	old := "nil"
	if d.localIP != nil {
		old = d.localIP.String()
	}
	log.Printf("[服务发现] 检测到本机 IP 变化: %s -> %s，重建 mDNS", old, newIP.String())
	d.localIP = newIP
	d.restartMDNS()
}

// restartMDNS 停止并重新启动 mDNS（IP 变化后调用）。注意：仅重建 mDNS socket/协程，
// 不再重启 watchIPChange（其在本进程生命周期内常驻于 discoveryStop），避免重复注册
// 导致 IP 监听句柄被旧协程的 defer 清掉而泄漏/失效。
func (d *ServiceDiscovery) restartMDNS() {
	d.stopMDNS()
	d.mdnsStop = make(chan struct{})
	d.startMDNS()
}

// Stop 停止服务发现
func (d *ServiceDiscovery) Stop() {
	d.mu.Lock()
	d.stopped = true
	d.mu.Unlock()

	if d.discoveryStop != nil {
		close(d.discoveryStop)
	}
	if d.ipNotifyCancel != nil {
		d.ipNotifyCancel()
		d.ipNotifyCancel = nil
	}
	d.stopMDNS()
	if d.udpConn != nil {
		close(d.udpStop)
		d.udpConn.Close()
		log.Printf("[服务发现-UDP] UDP 广播停止，共发送 %d 次广播", d.broadcastCnt)
	}
	d.wg.Wait()
}

// selfHealLoop 周期自检兜底：重新探测主 IPv4，与当前不一致即重建 mDNS。
// 作为各平台 IP 变化事件之外的保底——休眠/换网后若系统事件未及时触发（地址变更、
// 早期 APIPA 阶段、或事件丢失），最多 selfHealInterval 后必然恢复，不再依赖操作
// 系统通知的可靠性。
func (d *ServiceDiscovery) selfHealLoop() {
	d.wg.Add(1)
	defer d.wg.Done()
	ticker := time.NewTicker(selfHealInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.discoveryStop:
			return
		case <-ticker.C:
			d.onIPChanged(d.findLocalIPv4())
		}
	}
}

// isAPIPA 判断是否为 169.254.0.0/16 链路本地地址（多网卡/换网后残留，不能作为服务地址）
func isAPIPA(ip net.IP) bool {
	if ip4 := ip.To4(); ip4 != nil {
		return ip4[0] == 169 && ip4[1] == 254
	}
	return false
}

// privateScore 给私有地址更高优先级：192.168/10/172.16 在前，其余在后
func privateScore(ip net.IP) int {
	if ip4 := ip.To4(); ip4 != nil {
		switch {
		case ip4[0] == 192 && ip4[1] == 168:
			return 3
		case ip4[0] == 10:
			return 2
		case ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31:
			return 1
		}
	}
	return 0
}

// findLocalIPv4 找本机主网卡 IPv4（启动与 IP 变化重选共用同一逻辑）
func (d *ServiceDiscovery) findLocalIPv4() net.IP {
	// 1) 优先用默认路由出口 IP（连 8.8.8.8 的本地地址，仅查路由表不发包）
	if conn, err := net.Dial("udp", "8.8.8.8:80"); err == nil {
		ip := conn.LocalAddr().(*net.UDPAddr).IP
		conn.Close()
		if ip4 := ip.To4(); ip4 != nil && !ip4.IsLoopback() && !isAPIPA(ip4) {
			return ip4
		}
	}
	// 2) 兜底：收集所有非回环、非 APIPA 的 IPv4，按私有地址优先级排序取最优
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var candidates []net.IP
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			if ipNet, ok := addr.(*net.IPNet); ok {
				if ip4 := ipNet.IP.To4(); ip4 != nil && !ip4.IsLoopback() && !isAPIPA(ip4) {
					candidates = append(candidates, ip4)
				}
			}
		}
	}
	// 稳定排序：优先 192.168 > 10 > 172.16，其余靠后；同分保持原顺序
	sort.SliceStable(candidates, func(i, j int) bool {
		return privateScore(candidates[i]) > privateScore(candidates[j])
	})
	if len(candidates) > 0 {
		return candidates[0]
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

// allLocalIPv4 收集本机所有已启用、非回环、非 APIPA 的 IPv4 地址
func allLocalIPv4() []net.IP {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var ips []net.IP
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			if ipNet, ok := addr.(*net.IPNet); ok {
				if ip4 := ipNet.IP.To4(); ip4 != nil && !ip4.IsLoopback() && !isAPIPA(ip4) {
					ips = append(ips, ip4)
				}
			}
		}
	}
	return ips
}

// localIPForSubnet 根据查询来源 IP 选出本机同子网（同 /24）的 IPv4。
// 多网卡场景下（如外网走 192.168.0.x，LAN 走 192.168.1.x），默认路由出口 IP
// 可能是 192.168.0.127，但查询来自 192.168.1.33（LAN），此时应返回 192.168.1.x
// 的地址，否则对方拿到的是跨网段不可达的旧 IP。找不到同子网地址时回退 d.localIP。
func (d *ServiceDiscovery) localIPForSubnet(srcIP net.IP) net.IP {
	src4 := srcIP.To4()
	if src4 == nil {
		return d.localIP
	}
	for _, ip := range allLocalIPv4() {
		if ip[0] == src4[0] && ip[1] == src4[1] && ip[2] == src4[2] {
			return ip
		}
	}
	return d.localIP
}
