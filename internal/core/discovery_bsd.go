//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package core

import (
	"time"
)

// ipCheckSecsBSD BSD/macOS 下 IP 变化检测间隔（秒）。
// golang.org/x/net/route 仅支持抓取路由表（FetchRIB），不提供持续监听地址
// 变更的事件接口，故此处用短轮询兜底，兼顾实时性与零额外依赖。
const ipCheckSecsBSD = 10

// watchIPChange BSD/macOS 平台：周期性检测本机 IPv4 变化（短轮询）。
// 协程在本进程生命周期内常驻（阻塞于 discoveryStop），不被 restartMDNS 重复拉起。
func (d *ServiceDiscovery) watchIPChange() {
	d.wg.Add(1)
	defer d.wg.Done()
	ticker := time.NewTicker(ipCheckSecsBSD * time.Second)
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
