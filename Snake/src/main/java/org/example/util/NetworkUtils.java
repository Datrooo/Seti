package org.example.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class NetworkUtils {

    /**
     * Получает локальный IPv4 адрес (не loopback)
     */
    public static String getLocalIpAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();

            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();

                // Пропускаем loopback и неактивные интерфейсы
                if (iface.isLoopback() || !iface.isUp()) {
                    continue;
                }

                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();

                    // Ищем IPv4 адрес
                    if (!addr.isLoopbackAddress() && addr.getHostAddress().contains(".")) {
                        String ip = addr.getHostAddress();
                        Logger.info("Detected local IP: {}", ip);
                        return ip;
                    }
                }
            }
        } catch (SocketException e) {
            Logger.error("Failed to get local IP: {}", e.getMessage());
        }

        // Fallback
        return "127.0.0.1";
    }
}
