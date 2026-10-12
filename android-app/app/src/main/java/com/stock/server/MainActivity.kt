package com.stock.server.shell

import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.PowerManager
import android.provider.Settings
import android.text.InputType
import android.widget.EditText
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.appcompat.widget.SwitchCompat
import com.google.android.material.button.MaterialButton
import java.net.NetworkInterface

class MainActivity : AppCompatActivity() {
    private lateinit var tvStatus: TextView
    private lateinit var tvIp: TextView
    private lateinit var tvUrl: TextView
    private lateinit var etPort: EditText
    private lateinit var etSync: EditText
    private lateinit var swTdx: SwitchCompat
    private lateinit var swMqtt: SwitchCompat
    private lateinit var swDebug: SwitchCompat
    private lateinit var swAuto: SwitchCompat
    private lateinit var btnStart: MaterialButton
    private lateinit var btnStop: MaterialButton
    private lateinit var btnOpen: MaterialButton
    private lateinit var btnShare: MaterialButton
    private lateinit var btnBattery: MaterialButton

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        tvStatus = findViewById(R.id.tvStatus)
        tvIp     = findViewById(R.id.tvIp)
        tvUrl    = findViewById(R.id.tvUrl)
        etPort   = findViewById(R.id.etPort)
        etSync   = findViewById(R.id.etSync)
        swTdx    = findViewById(R.id.swTdx)
        swMqtt   = findViewById(R.id.swMqtt)
        swDebug  = findViewById(R.id.swDebug)
        swAuto   = findViewById(R.id.swAutoStart)
        btnStart = findViewById(R.id.btnStart)
        btnStop  = findViewById(R.id.btnStop)
        btnOpen  = findViewById(R.id.btnOpen)
        btnShare = findViewById(R.id.btnShare)
        btnBattery = findViewById(R.id.btnBattery)

        etPort.inputType = InputType.TYPE_CLASS_NUMBER
        etSync.inputType = InputType.TYPE_CLASS_NUMBER

        loadPrefs()
        btnStart.setOnClickListener { saveAndStart() }
        btnStop.setOnClickListener  { stopService() }
        btnOpen.setOnClickListener  { openBrowser() }
        btnShare.setOnClickListener { shareUrl() }
        btnBattery.setOnClickListener { requestBatteryIgnore() }

        // 若开机自启已经把服务拉起，状态同步
        refreshStatusUI()
    }

    override fun onResume() {
        super.onResume()
        refreshStatusUI()
    }

    private fun loadPrefs() {
        val p = getSharedPreferences(StockApp.PREF_NAME, Context.MODE_PRIVATE)
        etPort.setText(p.getInt(StockApp.KEY_PORT, 8080).toString())
        etSync.setText(p.getInt(StockApp.KEY_SYNC, 60).toString())
        swTdx.isChecked  = p.getBoolean(StockApp.KEY_USE_TDX, false)
        swMqtt.isChecked = p.getBoolean(StockApp.KEY_USE_MQTT, false)
        swDebug.isChecked = p.getBoolean(StockApp.KEY_DEBUG, false)
        swAuto.isChecked  = p.getBoolean(StockApp.KEY_AUTOSTART, true)
    }

    private fun savePrefs() {
        val port = etPort.text.toString().toIntOrNull() ?: 8080
        val sync = etSync.text.toString().toIntOrNull() ?: 60
        getSharedPreferences(StockApp.PREF_NAME, Context.MODE_PRIVATE).edit().apply {
            putInt(StockApp.KEY_PORT, port.coerceIn(1024, 65535))
            putInt(StockApp.KEY_SYNC, sync.coerceIn(0, 86400))
            putBoolean(StockApp.KEY_USE_TDX, swTdx.isChecked)
            putBoolean(StockApp.KEY_USE_MQTT, swMqtt.isChecked)
            putBoolean(StockApp.KEY_DEBUG, swDebug.isChecked)
            putBoolean(StockApp.KEY_AUTOSTART, swAuto.isChecked)
        }.apply()
    }

    private fun saveAndStart() {
        savePrefs()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            requestPermissions(arrayOf(android.Manifest.permission.POST_NOTIFICATIONS), 100)
        }
        val i = Intent(this, StockForegroundService::class.java)
            .setAction(StockForegroundService.ACTION_START)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) startForegroundService(i)
        else startService(i)
        Toast.makeText(this, "正在启动行情服务器...", Toast.LENGTH_SHORT).show()
        tvStatus.postDelayed({ refreshStatusUI() }, 2500)
    }

    private fun stopService() {
        val i = Intent(this, StockForegroundService::class.java)
            .setAction(StockForegroundService.ACTION_STOP)
        startService(i)
        Toast.makeText(this, "已请求停止", Toast.LENGTH_SHORT).show()
        tvStatus.postDelayed({ refreshStatusUI() }, 1500)
    }

    private fun port() = etPort.text.toString().toIntOrNull() ?: 8080

    private fun refreshStatusUI() {
        val running = StockForegroundService.runStatus == 1 ||
                getSharedPreferences(StockApp.PREF_NAME, Context.MODE_PRIVATE)
                    .getBoolean(StockApp.KEY_BIN_RUNNING, false)
        val ip = getWifiIp()
        val p = port()
        tvStatus.text = if (running) {
            getString(R.string.label_run_status_running, p)
        } else {
            getString(R.string.label_run_status)
        }
        tvStatus.setTextColor(getColor(if (running) R.color.ok else R.color.err))
        tvIp.text  = getString(R.string.label_local_ip) + "：${ip.ifBlank { getString(R.string.toast_no_ip) }}"
        val url = "http://${ip.ifBlank { "127.0.0.1" }}:$p/api/health"
        tvUrl.text = getString(R.string.label_access_url) + "：\n$url"
        btnStart.isEnabled = !running
        btnStop.isEnabled  = running
    }

    private fun openBrowser() {
        val ip = getWifiIp().ifBlank { "127.0.0.1" }
        val url = "http://$ip:${port()}/api/health"
        startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(url)))
    }

    private fun shareUrl() {
        val ip = getWifiIp().ifBlank { "127.0.0.1" }
        val body = buildString {
            append("【股票行情服务器已启动】\n")
            append("本机 IP: $ip  端口: ${port()}\n")
            append("健康检查: http://$ip:${port()}/api/health\n")
            append("实时行情示例: http://$ip:${port()}/api/realtime/600519\n")
            append("批量行情: http://$ip:${port()}/api/batch/quotes?codes=000001,601318,600519\n")
        }
        val i = Intent(Intent.ACTION_SEND).setType("text/plain")
            .putExtra(Intent.EXTRA_TEXT, body)
            .putExtra(Intent.EXTRA_SUBJECT, "股票行情 API 地址")
        startActivity(Intent.createChooser(i, "分享访问地址"))
    }

    private fun requestBatteryIgnore() {
        val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
        if (!pm.isIgnoringBatteryOptimizations(packageName)) {
            AlertDialog.Builder(this)
                .setTitle("电池优化白名单")
                .setMessage("为保证服务器在后台稳定运行，需要加入电池优化白名单。\n点击确定后选择「不限制」或「允许」。")
                .setPositiveButton("确定") { _, _ ->
                    startActivity(Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS,
                        Uri.parse("package:$packageName")))
                }
                .setNegativeButton("取消", null).show()
        } else {
            Toast.makeText(this, "已在电池优化白名单中 ✅", Toast.LENGTH_SHORT).show()
        }
    }

    private fun getWifiIp(): String {
        runCatching {
            for (ni in NetworkInterface.getNetworkInterfaces()) {
                for (ia in ni.interfaceAddresses) {
                    val addr = ia.address.hostAddress ?: continue
                    if (addr.startsWith("127.") || ':' in addr) continue
                    if (ni.name.startsWith("wlan") || ni.name.startsWith("ap") ||
                        ni.name.startsWith("eth") || ni.name.startsWith("rndis")) return addr
                }
            }
        }
        return ""
    }
}
