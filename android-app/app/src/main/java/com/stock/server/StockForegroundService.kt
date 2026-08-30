package com.stock.server.shell

import android.app.Notification
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
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

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        startForeground(StockApp.NOTIF_ID, buildNotification(getString(R.string.notify_running)))
        workingDir = File(filesDir, "go-server").also { it.mkdirs() }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_STOP -> stopServer()
            ACTION_START, null -> startServer()
        }
        return START_STICKY   // 被系统回收后自动重建保活
    }

    // —— 从 raw/arm64-v8a 释放 Go 二进制到 filesDir（必须私有目录才没 noexec）
    private fun ensureBinary(): File {
        val dir = File(filesDir, "bin").also { it.mkdirs() }
        val dest = File(dir, "go_stock_server")
        // 当 build-out 更新时，重新释放（按大小判断）
        val expectedSize = resources.openRawResource(R.raw.go_stock_server).use { it.available() }
        if (dest.exists() && dest.length() == expectedSize.toLong() && dest.canExecute()) {
            return dest
        }
        resources.openRawResource(R.raw.go_stock_server).use { ins ->
            dest.outputStream().use { outs -> ins.copyTo(outs) }
        }
        dest.setExecutable(true, false)
        Log.i("GoServer", "释放 Go 二进制: ${dest.absolutePath} (${dest.length()/1024/1024}MB)")
        return dest
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

        val bin = ensureBinary()
        val cmd = arrayListOf(bin.absolutePath).apply { addAll(args) }
        val logFile = File(workingDir, "server-${System.currentTimeMillis()}.log")
        val proc = ProcessBuilder(cmd)
            .directory(workingDir)
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))
            .apply { environment()["GODEBUG"] = "netdns=go" }   // 强制纯 Go DNS 解析
            .start()
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
        startForeground(StockApp.NOTIF_ID, buildNotification("$text · $status"))
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
