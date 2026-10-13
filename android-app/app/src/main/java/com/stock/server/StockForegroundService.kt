package com.stock.server.shell

import android.app.Notification
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.net.wifi.WifiManager
import android.os.Build
import android.os.IBinder
import android.util.Log
import androidx.core.app.NotificationCompat
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.atomic.AtomicReference

class StockForegroundService : Service() {
    companion object {
        const val ACTION_START = "com.stock.server.shell.START"
        const val ACTION_STOP  = "com.stock.server.shell.STOP"
        const val EXTRA_CONFIG = "extra_config"   // 启动参数 JSON（保留扩展）
        // run status：0=停止，1=运行，2=崩溃重启
        @Volatile var runStatus: Int = 0
            private set
        val lastLogs = ArrayDeque<String>(500)   // Activity 绑定可显示
        private val processRef = AtomicReference<Process?>(null)
    }

    private var logThread: Thread? = null
    private var workingDir: File? = null
    private var multicastLock: WifiManager.MulticastLock? = null

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        startForegroundCompat(buildNotification(getString(R.string.notify_running)))
        workingDir = File(filesDir, "go-server").also { it.mkdirs() }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_STOP -> stopServer()
            ACTION_START, null -> startServer()
        }
        return START_STICKY   // 被系统回收后自动重建保活
    }

    // —— 从 nativeLibraryDir 读取 Go 二进制
    // Android 10+ (targetSdk >= 29) SELinux 禁止执行 filesDir 下的二进制
    // (app_data_file 标签无 execute_no_trans 权限)。改用 jniLibs 方案：
    // 二进制命名为 libgoss.so 打入 APK，安装时提取到 nativeLibraryDir
    // (/data/app/<pkg>/lib/arm64/)，该目录有 app_library_file 标签，允许执行。
    private fun ensureBinary(): File {
        val nativeDir = applicationInfo.nativeLibraryDir
        val bin = File(nativeDir, "libgoss.so")
        if (!bin.exists() || !bin.canExecute()) {
            Log.e("GoServer", "nativeLibraryDir 二进制不存在或不可执行: ${bin.absolutePath}")
            Log.e("GoServer", "nativeLibraryDir=$nativeDir")
            throw IllegalStateException("Go 二进制未找到: ${bin.absolutePath}")
        }
        Log.i("GoServer", "Go 二进制: ${bin.absolutePath} (${bin.length()/1024/1024}MB)")
        return bin
    }

    private fun startServer() {
        if (processRef.get() != null) {
            Log.w("GoServer", "已在运行，忽略启动请求")
            return
        }
        val prefs = getSharedPreferences(StockApp.PREF_NAME, Context.MODE_PRIVATE)
        val port = prefs.getInt(StockApp.KEY_PORT, 8080)
        val sync = prefs.getInt(StockApp.KEY_SYNC, 60)
        val useTdx = prefs.getBoolean(StockApp.KEY_USE_TDX, false)
        val useMqtt = prefs.getBoolean(StockApp.KEY_USE_MQTT, false)
        val debug = prefs.getBoolean(StockApp.KEY_DEBUG, false)
        val dbPath = prefs.getString(StockApp.KEY_DB_PATH, "") ?: ""

        val args = mutableListOf<String>().apply {
            add("--host"); add("0.0.0.0")
            add("--port"); add(port.toString())
            add("--sync-interval"); add(sync.toString())
            add("--db");   add(dbPath)     // 空=纯内存
            if (useTdx)  add("--tdx")
            if (useMqtt) add("--mqtt")
            if (debug)   add("--debug")
        }

        val bin = try { ensureBinary() } catch (e: Exception) {
            Log.e("GoServer", "Go 二进制准备失败: ${e.message}", e)
            runStatus = 0
            prefs.edit().putBoolean(StockApp.KEY_BIN_RUNNING, false).apply()
            updateNotification("启动失败: ${e.message}", false)
            return
        }
        val cmd = arrayListOf(bin.absolutePath).apply { addAll(args) }
        val logFile = File(workingDir, "server-${System.currentTimeMillis()}.log")

        // 获取 WiFi MulticastLock，让 Go 二进制能接收 mDNS 多播查询包
        try {
            val wifi = getSystemService(Context.WIFI_SERVICE) as WifiManager
            multicastLock = wifi.createMulticastLock("stock-server-mdns").apply {
                setReferenceCounted(false)
                acquire()
            }
            Log.i("GoServer", "MulticastLock 已获取（mDNS 可收发）")
        } catch (e: Exception) {
            Log.w("GoServer", "MulticastLock 获取失败: ${e.message}（mDNS 仅宣告模式）")
        }

        val proc = try {
            ProcessBuilder(cmd)
                .directory(workingDir)
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))
                .apply { environment()["GODEBUG"] = "netdns=go" }   // 强制纯 Go DNS 解析
                .start()
        } catch (e: Exception) {
            Log.e("GoServer", "Go 二进制启动失败: ${e.message}", e)
            runStatus = 0
            prefs.edit().putBoolean(StockApp.KEY_BIN_RUNNING, false).apply()
            try { multicastLock?.let { if (it.isHeld) it.release() } } catch (_: Exception) {}
            multicastLock = null
            updateNotification("启动失败: ${e.message}", false)
            return
        }
        processRef.set(proc)
        runStatus = 1
        prefs.edit().putBoolean(StockApp.KEY_BIN_RUNNING, true).apply()

        // 同步 tail 到 lastLogs（给 Activity 展示）
        logThread = Thread { tailLogs(logFile) }.apply { isDaemon = true; start() }
        // 监控进程退出 → 若是异常退出则重跑最多 3 次
        Thread({
            val exitCode = runCatching { proc.waitFor() }.getOrDefault(-1)
            Log.w("GoServer", "进程退出，exitCode=$exitCode")
            runStatus = 0
            prefs.edit().putBoolean(StockApp.KEY_BIN_RUNNING, false).apply()
            processRef.set(null)
            updateNotification("服务器已退出（exit=$exitCode）", false)
            // 非正常退出（非 0/130 SIGINT/143 SIGTERM），延迟 5s 自动重启一次
            if (exitCode !in listOf(0, 130, 143)) {
                Thread.sleep(5000)
                if (prefs.getBoolean(StockApp.KEY_AUTOSTART, true)) {
                    Log.i("GoServer", "5s 后尝试自动重启")
                    startServer()
                }
            }
        }, "go-server-watcher").apply { isDaemon = true; start() }

        updateNotification(getString(R.string.notify_running) + "（:$port）", true)
        Log.i("GoServer", "已启动: ${cmd.joinToString(" ")}")
    }

    private fun tailLogs(file: File) {
        val reader: BufferedReader?
        try {
            reader = BufferedReader(InputStreamReader(file.inputStream()))
        } catch (t: Throwable) { return }
        reader.use { r ->
            while (runStatus == 1 && !Thread.interrupted()) {
                val line = r.readLine() ?: break
                synchronized(lastLogs) {
                    lastLogs.addLast(line)
                    while (lastLogs.size > 500) lastLogs.removeFirst()
                }
            }
        }
    }

    private fun stopServer() {
        runStatus = 0
        val proc = processRef.getAndSet(null) ?: return
        // 释放 MulticastLock
        try { multicastLock?.let { if (it.isHeld) it.release() } } catch (e: Exception) {}
        multicastLock = null
        runCatching {
            proc.destroy()        // 先发 SIGTERM（等价 Go 侧的）
            Thread.sleep(1500)
            if (proc.isAlive) proc.destroyForcibly()
        }
        getSharedPreferences(StockApp.PREF_NAME, Context.MODE_PRIVATE)
            .edit().putBoolean(StockApp.KEY_BIN_RUNNING, false).apply()
        updateNotification(getString(R.string.notify_stopped), false)
        stopForeground(STOP_FOREGROUND_REMOVE)
        stopSelf()
    }

    private fun updateNotification(text: String, running: Boolean) {
        val status = if (running) "运行中" else "已停止"
        startForegroundCompat(buildNotification("$text · $status"))
    }

    // Android 14 (API 34) 强制要求 startForeground 显式声明 foregroundServiceType,
    // 否则抛 ForegroundServiceTypeNotAllowed；低版本用双参数版本即可。
    private fun startForegroundCompat(notification: Notification) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            startForeground(StockApp.NOTIF_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE)
        } else {
            startForeground(StockApp.NOTIF_ID, notification)
        }
    }

    private fun buildNotification(text: String): Notification {
        val pi = PendingIntent.getActivity(
            this, 0, Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val stopIntent = Intent(this, StockForegroundService::class.java)
            .setAction(ACTION_STOP)
        val stopPi = PendingIntent.getService(
            this, 1, stopIntent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        return NotificationCompat.Builder(this, StockApp.CHANNEL_ID)
            .setContentTitle(getString(R.string.app_name))
            .setContentText(text)
            .setSmallIcon(android.R.drawable.ic_menu_upload_you_tube)
            .setColor(getColor(R.color.primary))
            .setContentIntent(pi)
            .setOngoing(true)
            .addAction(android.R.drawable.ic_menu_close_clear_cancel, "停止", stopPi)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
    }

    override fun onDestroy() {
        stopServer()
        super.onDestroy()
    }
}
