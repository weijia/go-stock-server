package com.stock.server.shell

import android.content.Context
import android.net.nsd.NsdManager
import android.net.nsd.NsdServiceInfo
import android.util.Log

/**
 * 用 Android NsdManager 注册 _stock-server._tcp mDNS 服务。
 *
 * 路径 1：Kotlin 端独立注册，不依赖 Go 的 SetMDNSRegistrar（跨进程无法调）。
 * 在 StockForegroundService.startServer() 里调 register()，stopServer() 里调 unregister()。
 *
 * 与 Go 侧 discovery.go 的 mdnsServiceType 常量对齐：
 *   服务类型：_stock-server._tcp
 *   TXT 记录：version / http_port / instance
 */
class StockNsdRegistrar(
    private val context: Context,
    private val httpPort: Int,
    private val instanceName: String = "stock-server-" + android.os.Build.MODEL.lowercase()
        .replace(Regex("[^a-z0-9-]"), "-")
        .take(20),
) {
    companion object {
        private const val TAG = "StockNsdRegistrar"
        // 必须与 Go 侧 internal/core/discovery.go 的 mdnsServiceType 完全一致
        const val SERVICE_TYPE = "_stock-server._tcp."
    }

    private var nsdManager: NsdManager? = null
    private var registrationListener: NsdManager.RegistrationListener? = null
    @Volatile private var registered = false

    /** 注册 mDNS 服务。可重复调用；已注册时先注销再注册。 */
    fun register() {
        synchronized(this) {
            if (registered) {
                Log.w(TAG, "已有注册，先注销后重新注册")
                unregisterInternal()
            }

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
                        Log.i(TAG, "mDNS 服务已注册: ${info.serviceName}$SERVICE_TYPE port=$httpPort")
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
        }
    }

    /** 注销 mDNS 服务。 */
    fun unregister() {
        synchronized(this) {
            unregisterInternal()
        }
    }

    private fun unregisterInternal() {
        registrationListener?.let { nsdManager?.unregisterService(it) }
        registrationListener = null
        nsdManager = null
        registered = false
    }
}
