package com.stock.server.shell

import android.app.Application
import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import android.os.Build

class StockApp : Application() {
    companion object {
        const val CHANNEL_ID = "stock_server_shell"
        const val NOTIF_ID = 1001
        const val PREF_NAME = "stock_server_prefs"
        // 配置项 key（与 internal/core.ServerConfig + flag 名对齐）
        const val KEY_PORT = "port"
        const val KEY_SYNC = "sync_interval"
        const val KEY_USE_TDX = "use_tdx"
        const val KEY_USE_MQTT = "use_mqtt"
        const val KEY_DEBUG = "debug"
        const val KEY_AUTOSTART = "autostart"
        const val KEY_DB_PATH = "db_path"  // 默认空（纯内存模式）
        const val KEY_BIN_RUNNING = "bin_running"
    }
    override fun onCreate() {
        super.onCreate()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val mgr = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
            val ch = NotificationChannel(CHANNEL_ID,
                getString(R.string.channel_name), NotificationManager.IMPORTANCE_LOW)
            ch.description = getString(R.string.channel_desc)
            ch.setShowBadge(false)
            mgr.createNotificationChannel(ch)
        }
    }
}
