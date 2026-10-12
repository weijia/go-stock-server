package com.stock.server.shell

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.os.Build
import android.util.Log

class BootReceiver : BroadcastReceiver() {
    override fun onReceive(ctx: Context, intent: Intent) {
        val action = intent.action ?: return
        Log.d("BootReceiver", "收到 action=$action")
        val prefs = ctx.getSharedPreferences(StockApp.PREF_NAME, Context.MODE_PRIVATE)
        if (!prefs.getBoolean(StockApp.KEY_AUTOSTART, true)) {
            Log.d("BootReceiver", "用户禁用自启，跳过")
            return
        }
        val svc = Intent(ctx, StockForegroundService::class.java)
            .setAction(StockForegroundService.ACTION_START)
        runCatching {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                ctx.startForegroundService(svc)
            } else {
                ctx.startService(svc)
            }
        }.onFailure { Log.e("BootReceiver", "启动失败", it) }
    }
}
