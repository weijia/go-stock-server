//go:build windows
// +build windows

// go-stock-server/discovery_windows.go - Windows 平台 mDNS 实现
// 使用 SO_REUSEADDR 选项绑定端口 5353，与系统 Dnscache 服务共享端口。
// 注意：Windows 上 SO_REUSEADDR 必须在 bind 之前设置，因此用 net.ListenConfig
// 的 Control 回调在创建 socket 时设置，再手动加入组播组（对齐 Python zeroconf
// 库的做法：SO_REUSEADDR + bind 0.0.0.0:5353 + IP_ADD_MEMBERSHIP）。
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/miekg/dns"
	"golang.org/x/net/ipv4"
	"golang.org/x/sys/windows"
)

// setReuseAddrControl 在 socket 创建（bind 前）时设置 SO_REUSEADDR，
// 使多个进程（系统 Dnscache / 其他 mDNS 实现）可共享 5353 端口。
func setReuseAddrControl(network, address string, c syscall.RawConn) error {
	var sockErr error
	if err := c.Control(func(fd uintptr) {
		sockErr = windows.SetsockoptInt(windows.Handle(fd), windows.SOL_SOCKET, windows.SO_REUSEADDR, 1)
	}); err != nil {
		return err
	}
	return sockErr
}

// stopMDNS 停止 mDNS 响应器
func (d *ServiceDiscovery) stopMDNS() {
	if d.mdnsConn != nil {
		close(d.mdnsStop)
		d.mdnsConn.Close()
		log.Println("[服务发现-mDNS] mDNS 响应器已停止")
	}
}

// startMDNS 启动 mDNS 宣告与查询响应（共享端口 5353）
func (d *ServiceDiscovery) startMDNS() {
	iface, err := d.findInterfaceByIP(d.localIP)
	if err != nil {
		log.Printf("[服务发现-mDNS] 找不到 IP %s 对应的网卡: %v", d.localIP, err)
		return
	}

	// 关键：Windows 上 SO_REUSEADDR 必须在 bind 之前设置，
	// 否则无法与系统 Dnscache / Python zeroconf 共享 5353 端口（收不到组播查询）。
	lc := net.ListenConfig{Control: setReuseAddrControl}
	pc, err := lc.ListenPacket(context.Background(), "udp4", "0.0.0.0:5353")
	if err != nil {
		log.Printf("[服务发现-mDNS] 绑定 5353 失败: %v", err)
		log.Println("[服务发现-mDNS] 仅使用 UDP 广播发现")
		return
	}
	conn, ok := pc.(*net.UDPConn)
	if !ok {
		log.Println("[服务发现-mDNS] 获取 UDP 连接失败，仅使用 UDP 广播发现")
		pc.Close()
		return
	}

	// 加入组播组并启用回环（对齐 Python zeroconf：SO_REUSEADDR + IP_ADD_MEMBERSHIP）
	group := &net.UDPAddr{IP: net.ParseIP(mdnsMulticastIPv4), Port: mdnsPort}
	mpc := ipv4.NewPacketConn(conn)
	if err := mpc.JoinGroup(iface, group); err != nil {
		log.Printf("[服务发现-mDNS] 加入组播组失败: %v", err)
		conn.Close()
		log.Println("[服务发现-mDNS] 仅使用 UDP 广播发现")
		return
	}
	if err := mpc.SetMulticastInterface(iface); err != nil {
		log.Printf("[服务发现-mDNS] 设置组播接口失败: %v", err)
	}
	if err := mpc.SetMulticastLoopback(true); err != nil {
		log.Printf("[服务发现-mDNS] 设置组播回环失败: %v", err)
	}

	d.mdnsConn = conn
	d.mdnsOK = true

	log.Println("[服务发现-mDNS] mDNS 服务已启动 (共享端口 5353)")
	log.Printf("[服务发现-mDNS] 服务实例: %s", d.fqdn)
	log.Printf("[服务发现-mDNS] 地址: %s:%d", d.localIP, d.httpPort)
	log.Println("[服务发现-mDNS] 网卡: " + iface.Name)

	// 查询响应协程
	d.wg.Add(1)
	go d.mdnsRespondLoop()

	// 主动宣告协程
	d.wg.Add(1)
	go d.mdnsAnnounceLoop()
}

// findInterfaceByIP 根据 IP 找到网卡
func (d *ServiceDiscovery) findInterfaceByIP(ip net.IP) (*net.Interface, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			if ipNet, ok := addr.(*net.IPNet); ok {
				if ipNet.IP.Equal(ip) {
					return &iface, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("no interface for %s", ip)
}

// mdnsRespondLoop 查询响应循环
func (d *ServiceDiscovery) mdnsRespondLoop() {
	defer d.wg.Done()
	buf := make([]byte, 1500)

	for {
		select {
		case <-d.mdnsStop:
			return
		default:
		}

		d.mdnsConn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, src, err := d.mdnsConn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if !strings.Contains(err.Error(), "closed") {
				log.Printf("[服务发现-mDNS] 读取错误: %v", err)
			}
			return
		}

		var msg dns.Msg
		if err := msg.Unpack(buf[:n]); err != nil {
			continue
		}

		if msg.Response || msg.Opcode != dns.OpcodeQuery || len(msg.Question) == 0 {
			continue
		}

		resp := d.buildMDNSResponse(&msg)
		if resp == nil {
			continue
		}

		data, _ := resp.Pack()
		// RFC 6762 §6.7：对 multicast 查询用 multicast 响应
		mcast := &net.UDPAddr{IP: net.ParseIP(mdnsMulticastIPv4), Port: mdnsPort}
		if _, err := d.mdnsConn.WriteToUDP(data, mcast); err != nil {
			log.Printf("[服务发现-mDNS] 组播响应发送失败: %v", err)
		}
		// 附加单播响应（zeroconf 等客户端绑定 5353 监听，单播能穿透部分组播过滤）
		if src.IP != nil {
			port := src.Port
			if port == 0 {
				port = mdnsPort
			}
			uni := &net.UDPAddr{IP: src.IP, Port: port}
			if _, err := d.mdnsConn.WriteToUDP(data, uni); err != nil {
				log.Printf("[服务发现-mDNS] 单播响应发送失败: %v", err)
			}
		}
		d.queryCnt++
		log.Printf("[服务发现-mDNS] 响应查询 #%d (%s)", d.queryCnt, src.IP)
	}
}

// mdnsAnnounceLoop 主动宣告循环
func (d *ServiceDiscovery) mdnsAnnounceLoop() {
	defer d.wg.Done()

	for i := 0; i < 3; i++ {
		d.sendMDNSAnnouncement()
		select {
		case <-d.mdnsStop:
			return
		case <-time.After(2 * time.Second):
		}
	}

	ticker := time.NewTicker(mdnsAnnounceSecs * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-d.mdnsStop:
			return
		case <-ticker.C:
			d.sendMDNSAnnouncement()
		}
	}
}

// sendMDNSAnnouncement 发送主动 mDNS 宣告报文
func (d *ServiceDiscovery) sendMDNSAnnouncement() {
	msg := new(dns.Msg)
	msg.Response = true
	msg.Authoritative = true

	msg.Answer = append(msg.Answer, &dns.PTR{
		Hdr: dns.RR_Header{Name: mdnsServiceType + ".local.", Rrtype: dns.TypePTR, Class: dns.ClassINET, Ttl: 120},
		Ptr: d.fqdn + ".",
	})

	msg.Answer = append(msg.Answer, &dns.SRV{
		Hdr:      dns.RR_Header{Name: d.fqdn + ".", Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: 120},
		Priority: 0, Weight: 0, Port: uint16(d.httpPort),
		Target: d.instanceName + ".local.",
	})

	msg.Answer = append(msg.Answer, &dns.TXT{
		Hdr: dns.RR_Header{Name: d.fqdn + ".", Rrtype: dns.TypeTXT, Class: dns.ClassINET, Ttl: 120},
		Txt: []string{
			"version=2.0",
			fmt.Sprintf("http_port=%d", d.httpPort),
			fmt.Sprintf("instance=%s", d.instanceName),
		},
	})

	msg.Extra = append(msg.Extra, &dns.A{
		Hdr: dns.RR_Header{Name: d.instanceName + ".local.", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 120},
		A:   d.localIP,
	})

	data, err := msg.Pack()
	if err != nil {
		return
	}

	dst := &net.UDPAddr{IP: net.ParseIP(mdnsMulticastIPv4), Port: mdnsPort}
	if _, err := d.mdnsConn.WriteToUDP(data, dst); err != nil {
		log.Printf("[服务发现-mDNS] 宣告发送失败: %v", err)
	}
}

// buildMDNSResponse 构建查询响应。
// 支持 PTR/SRV/TXT/A/ANY 查询；对每个匹配的查询返回完整记录集
// （PTR 响应附带 SRV/TXT，SRV/TXT 响应附带 A），保证 zeroconf 等客户端
// 一次查询即可拿到全部信息，无需二次查询。
func (d *ServiceDiscovery) buildMDNSResponse(query *dns.Msg) *dns.Msg {
	resp := &dns.Msg{}
	resp.Response = true
	resp.Authoritative = true
	resp.RecursionAvailable = false
	resp.SetReply(query)

	fqdn := strings.ToLower(d.fqdn + ".")
	svcType := strings.ToLower(mdnsServiceType + ".local.")
	host := strings.ToLower(d.instanceName + ".local.")

	srv := &dns.SRV{
		Hdr:      dns.RR_Header{Name: fqdn, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: 120},
		Priority: 0, Weight: 0, Port: uint16(d.httpPort),
		Target: d.instanceName + ".local.",
	}
	txt := &dns.TXT{
		Hdr: dns.RR_Header{Name: fqdn, Rrtype: dns.TypeTXT, Class: dns.ClassINET, Ttl: 120},
		Txt: []string{
			"version=2.0",
			fmt.Sprintf("http_port=%d", d.httpPort),
			fmt.Sprintf("instance=%s", d.instanceName),
		},
	}
	ptr := &dns.PTR{
		Hdr: dns.RR_Header{Name: svcType, Rrtype: dns.TypePTR, Class: dns.ClassINET, Ttl: 120},
		Ptr: d.fqdn + ".",
	}
	a := &dns.A{
		Hdr: dns.RR_Header{Name: host, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 120},
		A:   d.localIP,
	}

	matched := false
	for _, q := range query.Question {
		name := strings.ToLower(q.Name)
		// ANY 查询对任意名称返回全套记录
		if q.Qtype == dns.TypeANY {
			switch name {
			case svcType:
				resp.Answer = append(resp.Answer, ptr, srv, txt)
				matched = true
			case fqdn:
				resp.Answer = append(resp.Answer, srv, txt)
				matched = true
			case host:
				resp.Answer = append(resp.Answer, a)
				matched = true
			}
			continue
		}
		switch q.Qtype {
		case dns.TypePTR:
			if name == svcType {
				resp.Answer = append(resp.Answer, ptr, srv, txt)
				matched = true
			}
		case dns.TypeSRV:
			if name == fqdn {
				resp.Answer = append(resp.Answer, srv)
				matched = true
			}
		case dns.TypeTXT:
			if name == fqdn {
				resp.Answer = append(resp.Answer, txt)
				matched = true
			}
		case dns.TypeA:
			if name == host {
				resp.Answer = append(resp.Answer, a)
				matched = true
			}
		}
	}
	if !matched {
		return nil
	}
	// 附上 SRV Target 的 A 记录（RFC 6762 Additional Records，避免二次查询）
	resp.Extra = append(resp.Extra, a)
	return resp
}
