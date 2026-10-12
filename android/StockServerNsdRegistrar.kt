// go-stock-server/android/StockServerNsdRegistrar.kt
// 方案 A：Android 端用 NsdManager 注册 _stock-server._tcp，让 Go 侧不碰 5353 socket。
//
// 两种接入方式（按项目结构二选一）：
//
//   方式 1（推荐）：Android 端独立注册
//     - 适用：go-stock-server 仍是 main package（直接编译为 Android 可执行/Service）
//     - 做法：Android 端启动时直接调 NsdManager.registerService，端口填 Go HTTP 服务端口
//     - Kotlin 与 Go 之间无需互相调用，解耦最干净
//     - 本文件实现的就是这种模式
//
//   方式 2：通过 gomobile bind 注入到 Go 侧 ServiceDiscovery
//     - 适用：项目重构为 importable package（去掉 main 函数或拆出子 package）
//     - 做法：gomobile bind -target=android 生成 AAR，Kotlin 实现 MDNSRegistrar 接口
//     - 然后 Kotlin 端调用 ServiceDiscovery.setMDNSRegistrar(this) 注入
//     - Go 侧 Start() 自动调你的 Register / Unregister
//     - 文末 "方式 2 注入示例" 给出 Kotlin 端如何实现接口
package com.example.stockserver

import android.content.Context
import android.net.nsd.NsdManager
import android.net.nsd.NsdServiceInfo
import android.net.wifi.WifiManager
import android.util.Log
import java.net.Inet4Address
import java.net.NetworkInterface

/**
 * 用 Android NsdManager 注册 _stock-server._tcp mDNS 服务。
 *
 * 与 go-stock-server 内置 socket mDNS 实现对齐：
 *   服务类型：_stock-server._tcp （见 Go 侧 discovery.go 的 mdnsServiceType 常量）
 *   TXT 记录：version / http_port / instance（与 Go 侧 sendMDNSAnnouncement 的 TXT 对齐）
 *
 * 使用：
 *   val registrar = StockServerNsdRegistrar(context, httpPort = 8080)
 *   registrar.start()   // 注册 mDNS 服务
 *   ...
 *   registrar.stop()    // 注销
 */
class StockServerNsdRegistrar(
    private val context: Context,
    private val httpPort: Int,
    private val instanceName: String = "stock-server-" + android.os.Build.MODEL.lowercase()
        .replace(Regex("[^a-z0-9-]"), "-")
        .take(20),
) {
    companion object {
        private const val TAG = "StockNsdRegistrar"
        // 必须与 Go 侧 discovery.go 的 mdnsServiceType 完全一致
        const val SERVICE_TYPE = "_stock-server._tcp."
    }

    private var nsdManager: NsdManager? = null
    private var registrationListener: NsdManager.RegistrationListener? = null
    @Volatile private var registered = false

    // MulticastLock：注册端一般不需要（系统 NsdService 守护进程有权限），
    // 但有些 OEM ROM 实现严格，保险起见持有 lock 直到 stop()。
    private var multicastLock: WifiManager.MulticastLock? = null

    /**
     * 注册 mDNS 服务。可重复调用；已注册时先注销再注册。
     */
    fun start() {
        synchronized(this) {
            if (registered) {
                Log.w(TAG, "已有注册，先注销后重新注册")
                stopInternal()
            }
            acquireMulticastLock()

            val serviceInfo = NsdServiceInfo().apply {
                serviceName = instanceName
                serviceType = SERVICE_TYPE
                port = httpPort
                // 与 Go 侧 TXT 对齐，方便跨平台客户端统一解析
                setAttribute("version", "2.0")
                setAttribute("http_port", httpPort.toString())
                setAttribute("instance", instanceName)
            }

            nsdManager = (context.getSystemService(Context.NSD_SERVICE) as NsdManager).also { mgr ->
                registrationListener = object : NsdManager.RegistrationListener {
                    override fun onServiceRegistered(info: NsdServiceInfo) {
                        registered = true
                        Log.i(TAG, "mDNS 服务已注册: ${info.serviceName}.$SERVICE_TYPE port=$httpPort")
                    }

                    override fun onRegistrationFailed(info: NsdServiceInfo, errorCode: Int) {
                        registered = false
                        Log.e(TAG, "mDNS 注册失败 code=$errorCode svc=${info.serviceName}")
                    }

                    override fun onServiceUnregistered(info: NsdServiceInfo) {
                        registered = false
                        Log.i(TAG, "mDNS 服务已注销: ${info.serviceName}")
                    }

                    override fun onUnregistrationFailed(info: NsdServiceInfo, errorCode: Int) {
                        Log.e(TAG, "mDNS 注销失败 code=$errorCode svc=${info.serviceName}")
                    }
                }
                mgr.registerService(serviceInfo, NsdManager.PROTOCOL_DNS_SD, registrationListener)
            }

            Log.i(TAG, "提交 NsdManager 注册请求: $instanceName$SERVICE_TYPE port=$httpPort")
            Log.i(TAG, "本机 IPv4: ${localIPv4() ?: "未获取到"}")
        }
    }

    /**
     * 注销 mDNS 服务。
     */
    fun stop() {
        synchronized(this) {
            stopInternal()
            releaseMulticastLock()
        }
    }

    private fun stopInternal() {
        registrationListener?.let { nsdManager?.unregisterService(it) }
        registrationListener = null
        nsdManager = null
        registered = false
    }

    private fun acquireMulticastLock() {
        if (multicastLock != null) return
        val wifi = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
            ?: return
        val lock = wifi.createMulticastLock("go-stock-server-mdns").apply {
            setReferenceCounted(false)
            try { acquire() } catch (e: Exception) {
                Log.w(TAG, "MulticastLock acquire 失败: ${e.message}")
            }
        }
        multicastLock = lock
        Log.d(TAG, "MulticastLock 已持有")
    }

    private fun releaseMulticastLock() {
        multicastLock?.let {
            try { if (it.isHeld) it.release() } catch (e: Exception) {
                Log.w(TAG, "MulticastLock release 失败: ${e.message}")
            }
        }
        multicastLock = null
    }

    /** 收集本机非回环、非 APIPA 的 IPv4 地址，便于调试与日志输出。 */
    private fun localIPv4(): String? {
        return try {
            NetworkInterface.getNetworkInterfaces()
                .asSequence()
                .filter { it.isUp && !it.isLoopback }
                .flatMap { it.inetAddresses.asSequence() }
                .filterIsInstance<Inet4Address>()
                .firstOrNull { !it.isLoopbackAddress &&
                    !(it.hostAddress?.startsWith("169.254.") == true) }
                ?.hostAddress
        } catch (e: Exception) {
            null
        }
    }

    // ============================================================
    // 方式 2 注入示例：项目重构为 importable package 后，
    // 用 gomobile bind 生成 AAR，让 Kotlin 实现 Go 的 MDNSRegistrar 接口
    // 并通过 ServiceDiscovery.setMDNSRegistrar(this) 注入到 Go 侧。
    //
    // 假设 Go 侧 AAR 包名为 stockserver，暴露：
    //   public interface MDNSRegistrar {
    //       Error register(String serviceType, String instanceName, long port);
    //       Error unregister();
    //   }
    //   public final class ServiceDiscovery {
    //       public void setMDNSRegistrar(MDNSRegistrar r) { ... }
    //   }
    //
    // Kotlin 端这样实现：
    // ============================================================
    //
    // class NsdRegistrarAdapter(private val ctx: Context) : stockserver.MDNSRegistrar {
    //     private val delegate = StockServerNsdRegistrar(ctx, httpPort = 8080)
    //
    //     override fun register(serviceType: String, instanceName: String, port: Long): stockserver.Error? {
    //         // serviceType 应为 "_stock-server._tcp"，可忽略（delegate 已硬编码同款常量）
    //         // port 应与 Go HTTP server 端口一致
    //         delegate.start()
    //         return null  // 返回 null 表示成功
    //     }
    //
    //     override fun unregister(): stockserver.Error? {
    //         delegate.stop()
    //         return null
    //     }
    // }
    //
    // 在 Application/Service onCreate 里：
    //   val discovery = stockserver.ServiceDiscovery.newServiceDiscovery(8080)
    //   discovery.setMDNSRegistrar(NsdRegistrarAdapter(this))
    //   discovery.start()
}
