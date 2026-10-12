// go-stock-server/mdns_registrar.go - mDNS 注册器抽象（方案 A）
//
// 设计动机：
//   PC（Linux/macOS/Windows）直接绑定 5353 组播端口跑 mDNS 是 OK 的；
//   但 Android 上走 Go 原生 socket 不可行——
//     1) 5353 端口被系统 NsdService / mdnsd 占用，bind 易 EADDRINUSE
//     2) WiFi 驱动默认过滤组播包，必须用 Java/Kotlin 调
//        WifiManager.createMulticastLock() 才能收发，纯 Go 拿不到
//     3) AndroidManifest 还需声明 CHANGE_WIFI_MULTICAST_STATE 等权限
//
// 因此把 mDNS 注册从「ServiceDiscovery 内部直接绑 socket」抽象为可注入的
// MDNSRegistrar 接口：
//   · PC 平台：不注入 registrar，自动回退到内置 socket 实现
//     （discovery_mdns.go / discovery_windows.go 的 startMDNS/stopMDNS）
//   · Android 平台：通过 SetMDNSRegistrar 注入 Kotlin 端 NsdManager 包装，
//     Go 侧完全不碰 5353 socket，规避权限/端口冲突问题
package main

import "log"

// MDNSRegistrar mDNS 注册器接口。
//
// Android 端用 NsdManager.registerService 实现并注入；PC 端可不实现，
// ServiceDiscovery 会自动回退到内置 socket 版本。
//
// 注意：接口签名刻意只使用 gomobile bind 可直接导出的类型
// （string / int / error），不使用 map、切片、interface 字段，方便
// 通过 gomobile bind 桥接到 Kotlin。
type MDNSRegistrar interface {
	// Register 注册 mDNS 服务。
	//   serviceType: 服务类型，如 "_stock-server._tcp"
	//   instanceName: 实例名（通常为本机 hostname 或自定义名）
	//   port: HTTP 服务端口
	Register(serviceType string, instanceName string, port int) error
	// Unregister 注销已注册的服务。
	Unregister() error
}

// SetMDNSRegistrar 注入外部 mDNS 注册器（方案 A 的入口）。
//
// 调用时机：
//   · Android 端：在 ServiceDiscovery.Start() 之前调用（推荐在 Application
//     onCreate 或 Service.onCreate 中先 SetMDNSRegistrar 再启动 Go 端逻辑）
//   · PC 端：无需调用，留空时 Start() 自动回退到内置 socket 实现
//
// 调用后立即生效；若 ServiceDiscovery 已经在跑内置 socket mDNS，
// 会先停掉 socket 版本，再切到外部 registrar（一般不在运行期切换）。
func (d *ServiceDiscovery) SetMDNSRegistrar(r MDNSRegistrar) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.registrar = r
	log.Printf("[服务发现] 已注入外部 mDNS registrar: %T", r)
}

// tryExternalMDNS 尝试用注入的 registrar 注册 mDNS。
// 返回 true 表示已用外部 registrar 成功注册（无需再启动 socket 版本）；
// 返回 false 表示无 registrar 或注册失败，调用方应回退到内置 socket 实现。
//
// 调用方：Start() 与 restartMDNS() 共用。
func (d *ServiceDiscovery) tryExternalMDNS() bool {
	d.mu.Lock()
	r := d.registrar
	d.mu.Unlock()
	if r == nil {
		return false
	}
	if err := r.Register(mdnsServiceType, d.instanceName, d.httpPort); err != nil {
		log.Printf("[服务发现-mDNS] 外部 registrar 注册失败: %v，回退到内置 socket 实现", err)
		return false
	}
	d.mdnsOK = true
	log.Printf("[服务发现-mDNS] 已通过外部 registrar 注册: type=%s instance=%s port=%d",
		mdnsServiceType, d.instanceName, d.httpPort)
	log.Println("[服务发现-mDNS] Android NsdManager 已接管 mDNS 宣告")
	return true
}

// unregisterExternalMDNS 注销外部 registrar（若已注入）。
// 返回 true 表示已用外部 registrar 注销（无需再调 stopMDNS 关 socket）。
func (d *ServiceDiscovery) unregisterExternalMDNS() bool {
	d.mu.Lock()
	r := d.registrar
	d.mu.Unlock()
	if r == nil {
		return false
	}
	if err := r.Unregister(); err != nil {
		log.Printf("[服务发现-mDNS] 外部 registrar 注销失败: %v", err)
	} else {
		log.Println("[服务发现-mDNS] 外部 registrar 已注销")
	}
	return true
}
