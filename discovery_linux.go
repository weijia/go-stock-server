//go:build linux

package main

import (
	"time"
)

// ipCheckSecsLinux Linux 下 IP 变化检测间隔（秒）。
// Linux 无零依赖的内核地址变更事件 API（需 netlink 库），故用短轮询兜底，
// 兼顾实时性与零额外依赖。
const ipCheckSecsLinux = 10

// watchIPChange Linux 平台：周期性检测本机 IPv4 变化（短轮询）。
func (d *ServiceDiscovery) watchIPChange() {
	ticker := time.NewTicker(ipCheckSecsLinux * time.Second)
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
