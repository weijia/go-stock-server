package com.stock.server.gui;

import android.app.Application;
import android.content.Context;
import android.net.wifi.WifiManager;
import android.util.Log;

/**
 * 在 App 启动时获取 WifiManager.MulticastLock。
 *
 * Android WiFi 驱动默认过滤多播包以省电，即使 Manifest 声明了
 * CHANGE_WIFI_MULTICAST_STATE 权限，也必须编程式获取 MulticastLock
 * 才能让 Go 运行时的 net.ListenMulticastUDP 真正收到 mDNS 查询包。
 *
 * 本类在 Application.onCreate()（早于所有 Activity / Service）中获取锁，
 * 锁的生命周期与 App 进程一致，Go 侧的 mDNS 响应器可正常收发。
 */
public class MulticastLockApp extends Application {
    private WifiManager.MulticastLock multicastLock;

    @Override
    public void onCreate() {
        super.onCreate();
        try {
            WifiManager wifi = (WifiManager) getSystemService(Context.WIFI_SERVICE);
            multicastLock = wifi.createMulticastLock("stock-server-mdns");
            multicastLock.setReferenceCounted(false);
            multicastLock.acquire();
            Log.i("StockServer", "MulticastLock acquired (mDNS ready)");
        } catch (Exception e) {
            Log.w("StockServer", "MulticastLock failed: " + e.getMessage());
        }
    }
}
