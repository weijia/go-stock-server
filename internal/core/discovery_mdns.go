//go:build !windows
// +build !windows

// go-stock-server/discovery_mdns.go - 非 Windows 平台的 mDNS 实现
// 直接绑定 5353 组播端口，自行处理 mDNS 查询/宣告
package core

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"
)

// stopMDNS 停止 mDNS 响应器
func (d *ServiceDiscovery) stopMDNS() {
	if d.mdnsConn != nil {
		close(d.mdnsStop)
		d.mdnsConn.Close()
		log.Println("[服务发现-mDNS] mDNS 响应器已停止")
	}
}

// startMDNS 启动 mDNS 响应器 + 主动宣告
func (d *ServiceDiscovery) startMDNS() {
	iface, err := d.findInterfaceByIP(d.localIP)
	if err != nil {
		log.Printf("[服务发现-mDNS] 找不到 IP %s 对应的网卡: %v", d.localIP, err)
		return
	}

	conn, err := net.ListenMulticastUDP("udp4", iface, &net.UDPAddr{
		IP:   net.ParseIP(mdnsMulticastIPv4),
		Port: mdnsPort,
	})
	if err != nil {
		log.Printf("[服务发现-mDNS] 组播绑定失败: %v", err)
		return
	}
	d.mdnsConn = conn
	d.mdnsOK = true

	log.Println("[服务发现-mDNS] mDNS 响应器已启动")
	log.Printf("[服务发现-mDNS] 组播组: %s:%d (接口 %s)", mdnsMulticastIPv4, mdnsPort, iface.Name)
	log.Printf("[服务发现-mDNS] 服务实例: %s", d.fqdn)
	log.Printf("[服务发现-mDNS] 地址: %s:%d", d.localIP, d.httpPort)
	log.Println("[服务发现-mDNS] Android NSD / iOS Bonjour 可发现此服务")

	// 查询响应协程
	d.wg.Add(1)
	go d.mdnsRespondLoop()

	// 主动宣告协程（即使收不到查询也能被发现）
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

		resp := d.buildMDNSResponse(&msg, src.IP)
		if resp == nil {
			continue
		}

		data, _ := resp.Pack()
		dst := &net.UDPAddr{IP: src.IP, Port: mdnsPort}
		if _, err := d.mdnsConn.WriteToUDP(data, dst); err != nil {
			log.Printf("[服务发现-mDNS] 发送失败: %v", err)
		} else {
			d.queryCnt++
			log.Printf("[服务发现-mDNS] 响应查询 #%d (%s) -> 应答IP %s", d.queryCnt, src.IP, d.localIPForSubnet(src.IP))
		}
	}
}

// mdnsAnnounceLoop 主动宣告循环（定时发送 unsolicited mDNS 响应）
func (d *ServiceDiscovery) mdnsAnnounceLoop() {
	defer d.wg.Done()

	// 启动时立即宣告 3 次（mDNS 规范要求快速启动）
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

	// PTR 记录
	msg.Answer = append(msg.Answer, &dns.PTR{
		Hdr: dns.RR_Header{Name: mdnsServiceType + ".local.", Rrtype: dns.TypePTR, Class: dns.ClassINET, Ttl: 120},
		Ptr: d.fqdn + ".",
	})

	// SRV 记录
	msg.Answer = append(msg.Answer, &dns.SRV{
		Hdr:      dns.RR_Header{Name: d.fqdn + ".", Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: 120},
		Priority: 0, Weight: 0, Port: uint16(d.httpPort),
		Target: d.instanceName + ".local.",
	})

	// TXT 记录
	msg.Answer = append(msg.Answer, &dns.TXT{
		Hdr: dns.RR_Header{Name: d.fqdn + ".", Rrtype: dns.TypeTXT, Class: dns.ClassINET, Ttl: 120},
		Txt: []string{
			fmt.Sprintf("version=2.0"),
			fmt.Sprintf("http_port=%d", d.httpPort),
			fmt.Sprintf("instance=%s", d.instanceName),
		},
	})

	// A 记录 (附加)
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

// buildMDNSResponse 构建查询响应
func (d *ServiceDiscovery) buildMDNSResponse(query *dns.Msg, srcIP net.IP) *dns.Msg {
	resp := &dns.Msg{}
	resp.Response = true
	resp.Authoritative = true
	resp.RecursionAvailable = false
	resp.SetReply(query)

	// 按查询来源网段选出本机应答 IP（多网卡/跨网段时返回可直达的地址，而非默认出口 IP）
	answerIP := d.localIPForSubnet(srcIP)

	matched := false
	for _, q := range query.Question {
		name := strings.ToLower(q.Name)
		switch q.Qtype {
		case dns.TypePTR:
			if name == mdnsServiceType+".local." {
				resp.Answer = append(resp.Answer, &dns.PTR{
					Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypePTR, Class: dns.ClassINET, Ttl: 120},
					Ptr: d.fqdn + ".",
				})
				matched = true
			}
		case dns.TypeSRV:
			if name == d.fqdn+"." {
				resp.Answer = append(resp.Answer, &dns.SRV{
					Hdr:      dns.RR_Header{Name: q.Name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: 120},
					Priority: 0, Weight: 0, Port: uint16(d.httpPort),
					Target: d.instanceName + ".local.",
				})
				resp.Extra = append(resp.Extra, &dns.A{
					Hdr: dns.RR_Header{Name: d.instanceName + ".local.", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 120},
					A:   answerIP,
				})
				matched = true
			}
		case dns.TypeA:
			if name == d.instanceName+".local." {
				resp.Answer = append(resp.Answer, &dns.A{
					Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 120},
					A:   answerIP,
				})
				matched = true
			}
		case dns.TypeTXT:
			if name == d.fqdn+"." {
				resp.Answer = append(resp.Answer, &dns.TXT{
					Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeTXT, Class: dns.ClassINET, Ttl: 120},
					Txt: []string{
						fmt.Sprintf("version=2.0"),
						fmt.Sprintf("http_port=%d", d.httpPort),
						fmt.Sprintf("instance=%s", d.instanceName),
					},
				})
				matched = true
			}
		}
	}
	if !matched {
		return nil
	}
	return resp
}
