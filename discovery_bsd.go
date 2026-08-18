//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package main

import (
	"time"
)

// ipCheckSecsBSD BSD/macOS 下 IP 变化检测间隔（秒）。
// golang.org/x/net/route 仅支持抓取路由表（FetchRIB），不提供持续监听地址
// 变更的事件接口，故此处用短轮询兜底，兼顾实时性与零额外依赖。
const ipCheckSecsBSD = 10

// watchIPChange BSD/macOS 平台：周期性检测本机 IPv4 变化（短轮询）。
func (d *ServiceDiscovery) watchIPChange() {
	ticker := time.NewTicker(ipCheckSecsBSD * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-d.mdnsStop:
			return
		case <-ticker.C:
			d.onIPChanged(d.findLocalIPv4())
		}
	}
}
