//go:build android
// +build android

// go-stock-server/discovery_android.go - Android 平台占位实现
//
// Android 上不使用 Go 内置 socket 绑定 5353 跑 mDNS（原因见 mdns_registrar.go
// 顶部注释），改由 Kotlin 端用 NsdManager.registerService 注册服务，通过
// ServiceDiscovery.SetMDNSRegistrar 注入到 Go 侧。
//
// 本文件提供 ServiceDiscovery 在 Android 下仍需调用的几个方法的占位实现：
//   · startMDNS：仅打印警告，不绑 socket（理论上 Start() 已先尝试外部 registrar，
//     走到这里说明未注入 registrar，应提示用户在 Android 端调 SetMDNSRegistrar）
//   · stopMDNS：空实现（无 socket 可关）
//   · watchIPChange：短轮询兜底（Android 没有 Go 层零依赖的 IP 变更事件 API，
//     复用 Linux 的轮询策略）
package main

import (
	"fmt"
	"log"
	"net"
	"time"
)

// ipCheckSecsAndroid Android 下 IP 变化检测间隔（秒）。
// 与 Linux 一致用短轮询兜底；Android 上 IP 变化通常由系统 ConnectivityManager
// 通知，Kotlin 端可进一步通过 SetMDNSRegistrar 触发重注册。
const ipCheckSecsAndroid = 10

// startMDNS Android 平台占位：不绑 5353 socket。
// 走到这里说明未通过 SetMDNSRegistrar 注入 NsdManager 包装；打印提示并保持
// mdnsOK=false，UDP 广播仍可用作回退发现方式。
func (d *ServiceDiscovery) startMDNS() {
	log.Println("[服务发现-mDNS] Android 平台不内置 socket mDNS 实现")
	log.Println("[服务发现-mDNS] 请在 Android 端用 NsdManager 注册服务，并调用 SetMDNSRegistrar 注入")
	log.Printf("[服务发现-mDNS] 注册参数: type=%s instance=%s port=%d",
		mdnsServiceType, d.instanceName, d.httpPort)
	log.Println("[服务发现-mDNS] 仅使用 UDP 广播发现作为回退")
}

// stopMDNS Android 平台占位：无 socket 可关，直接返回。
func (d *ServiceDiscovery) stopMDNS() {
	if d.mdnsConn != nil {
		// 理论上不会进入此分支（Android 不创建 socket），保险起见仍处理
		close(d.mdnsStop)
		d.mdnsConn.Close()
		log.Println("[服务发现-mDNS] mDNS 响应器已停止")
	}
}

// findInterfaceByIP Android 平台占位：未使用（socket 路径不走），返回 nil + 错误。
// 保留方法签名仅为通过编译，实际不调用。
func (d *ServiceDiscovery) findInterfaceByIP(ip net.IP) (*net.Interface, error) {
	return nil, fmt.Errorf("findInterfaceByIP not supported on Android")
}

// watchIPChange Android 平台：周期性检测本机 IPv4 变化（短轮询兜底）。
// 协程在本进程生命周期内常驻（阻塞于 discoveryStop），不被 restartMDNS 重复拉起。
func (d *ServiceDiscovery) watchIPChange() {
	d.wg.Add(1)
	defer d.wg.Done()
	ticker := time.NewTicker(ipCheckSecsAndroid * time.Second)
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
