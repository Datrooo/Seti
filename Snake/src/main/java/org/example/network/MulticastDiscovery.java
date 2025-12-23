package org.example.network;

import org.example.protocol.SnakesProto;
import org.example.util.Config;
import org.example.util.Logger;

import java.io.IOException;
import java.net.*;
import java.util.Enumeration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class MulticastDiscovery {
    private final String multicastAddress;
    private final int multicastPort;
    private MulticastSocket socket;
    private InetAddress group;
    private volatile boolean running;
    private final CopyOnWriteArrayList<Consumer<AnnouncementWithAddress>> listeners;

    public MulticastDiscovery() {
        this.multicastAddress = Config.MULTICAST_ADDRESS;
        this.multicastPort = Config.MULTICAST_PORT;
        this.listeners = new CopyOnWriteArrayList<>();
        this.running = false;
    }

    public void start() throws IOException {
        socket = new MulticastSocket(multicastPort);
        socket.setReuseAddress(true);
        socket.setTimeToLive(255); // TTL для multicast (255 = максимальный, для работы через loopback)
        group = InetAddress.getByName(multicastAddress);
        
        Logger.info("Looking for suitable multicast interface...");
        
        // Присоединяемся к multicast группе
        NetworkInterface useInterface = null;
        try {
            NetworkInterface networkInterface = getNetworkInterface();
            if (networkInterface != null) {
                socket.joinGroup(new InetSocketAddress(group, multicastPort), networkInterface);
                useInterface = networkInterface;
                Logger.info("Joined multicast group using interface: {} ({})", 
                        networkInterface.getDisplayName(), networkInterface.getName());
            } else {
                // Нет подходящего интерфейса, пробуем loopback
                Logger.warn("No suitable network interface found, trying loopback");
                useInterface = NetworkInterface.getByInetAddress(InetAddress.getLoopbackAddress());
                socket.joinGroup(new InetSocketAddress(group, 0), useInterface);
                Logger.info("Joined multicast group using loopback interface");
            }
        } catch (Exception e) {
            // Последний fallback - без указания интерфейса
            Logger.warn("Failed to join with specific interface, trying default: {}", e.getMessage());
            try {
                useInterface = NetworkInterface.getByInetAddress(InetAddress.getLoopbackAddress());
                socket.joinGroup(new InetSocketAddress(group, 0), useInterface);
                Logger.info("Joined multicast group using loopback fallback");
            } catch (Exception e2) {
                socket.joinGroup(new InetSocketAddress(group, 0), null);
                Logger.info("Joined multicast group using system default");
            }
        }
        
        // Устанавливаем интерфейс для отправки multicast
        if (useInterface != null) {
            socket.setNetworkInterface(useInterface);
            Logger.info("Set multicast send interface to: {} ({})", 
                    useInterface.getDisplayName(), useInterface.getName());
        }
        
        running = true;
        Thread.ofVirtual().start(this::listenLoop);
        Logger.info("Multicast discovery started on {}:{}", multicastAddress, multicastPort);
    }

    public void stop() {
        running = false;

        try {
            if (socket != null && group != null) {
                try {
                    NetworkInterface networkInterface = getNetworkInterface();
                    if (networkInterface != null) {
                        socket.leaveGroup(new InetSocketAddress(group, multicastPort), networkInterface);
                    } else {
                        socket.leaveGroup(new InetSocketAddress(group, 0), null);
                    }
                } catch (IOException e) {
                    Logger.debug("Error leaving multicast group: {}", e.getMessage());
                }
                socket.close();
            }
            Logger.info("Multicast discovery stopped");
        } catch (Exception e) {
            Logger.error("Error stopping multicast discovery: {}", e.getMessage());
        }
    }

    private void listenLoop() {
        byte[] buffer = new byte[65536];
        Logger.info("Multicast listen loop started");

        while (running) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                byte[] data = new byte[packet.getLength()];
                System.arraycopy(buffer, 0, data, 0, packet.getLength());

                // Получаем адрес отправителя
                InetSocketAddress senderAddress = new InetSocketAddress(
                        packet.getAddress(),
                        Config.DEFAULT_PORT
                );

                Logger.debug("Received multicast packet from {} (actual port: {})", 
                        packet.getAddress(), packet.getPort());

                SnakesProto.GameMessage message = SnakesProto.GameMessage.parseFrom(data);

                if (message.hasAnnouncement()) {
                    Logger.debug("Received multicast announcement from {}", senderAddress);
                    for (SnakesProto.GameAnnouncement game :
                            message.getAnnouncement().getGamesList()) {
                        notifyListeners(game, senderAddress);
                        Logger.info("Discovered game: {} from {}", game.getGameName(), senderAddress);
                    }
                }

            } catch (IOException e) {
                if (running) {
                    Logger.error("Error receiving multicast: {}", e.getMessage());
                }
            }
        }
        Logger.info("Multicast listen loop stopped");
    }

    public void addAnnouncementListener(Consumer<AnnouncementWithAddress> listener) {
        listeners.add(listener);
    }


    private void notifyListeners(SnakesProto.GameAnnouncement announcement, InetSocketAddress senderAddress) {
        AnnouncementWithAddress data = new AnnouncementWithAddress(announcement, senderAddress);
        for (Consumer<AnnouncementWithAddress> listener : listeners) {
            try {
                listener.accept(data);
            } catch (Exception e) {
                Logger.error("Error in announcement listener: {}", e.getMessage());
            }
        }
    }

    private NetworkInterface getNetworkInterface() throws SocketException {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        
        // Приоритет 1: Ищем активные не-loopback интерфейсы с IPv4
        while (interfaces.hasMoreElements()) {
            NetworkInterface iface = interfaces.nextElement();
            if (!iface.isLoopback() && iface.isUp() && iface.supportsMulticast()) {
                // Проверяем наличие IPv4 адресов
                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    if (addr instanceof java.net.Inet4Address && !addr.isLoopbackAddress()) {
                        Logger.info("Found suitable interface: {} with IPv4: {}", 
                                iface.getDisplayName(), addr.getHostAddress());
                        return iface;
                    }
                }
            }
        }
        
        // Приоритет 2: Любой multicast интерфейс
        interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface iface = interfaces.nextElement();
            if (iface.isUp() && iface.supportsMulticast()) {
                Logger.warn("Using fallback interface: {}", iface.getDisplayName());
                return iface;
            }
        }
        
        return null;
    }

    public void sendAnnouncement(SnakesProto.GameMessage announcement) {
        if (socket == null || socket.isClosed()) {
            Logger.warn("Multicast socket not available, cannot send announcement");
            return;
        }

        try {
            byte[] data = announcement.toByteArray();
            DatagramPacket packet = new DatagramPacket(
                    data,
                    data.length,
                    group,
                    multicastPort
            );

            socket.send(packet);
            Logger.info("Sent multicast announcement to {}:{} ({} bytes)", 
                    group.getHostAddress(), multicastPort, data.length);

        } catch (IOException e) {
            Logger.error("Error sending multicast announcement: {}", e.getMessage());
        }
    }

    public record AnnouncementWithAddress( // передача announcement с адресом отправителя
            SnakesProto.GameAnnouncement announcement,
            InetSocketAddress senderAddress
    ) {}
}
