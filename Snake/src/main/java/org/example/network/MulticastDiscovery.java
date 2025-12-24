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

    public void start() {
        try {
            socket = new MulticastSocket(multicastPort);
            socket.setReuseAddress(true);
            socket.setTimeToLive(255);
            group = InetAddress.getByName(multicastAddress);
            
            NetworkInterface iface = getNetworkInterface();
            
            try {
                socket.joinGroup(new InetSocketAddress(group, multicastPort), iface);
                if (iface != null) {
                    socket.setNetworkInterface(iface);
                    Logger.info("Multicast started on {} ({})", iface.getDisplayName(), iface.getName());
                } else {
                    Logger.info("Multicast started on system default interface");
                }
            } catch (IOException e) {
                Logger.warn("Multicast join failed (game discovery disabled): {}", e.getMessage());
                if (socket != null) socket.close();
                socket = null;
                return;
            }
            
            running = true;
            Thread.ofVirtual().start(this::listenLoop);
        } catch (Exception e) {
            Logger.warn("Multicast initialization failed (game discovery disabled): {}", e.getMessage());
            socket = null;
        }
    }

    public void stop() {
        running = false;
        if (socket != null) {
            socket.close();
        }
    }

    private void listenLoop() {
        byte[] buffer = new byte[65536];

        while (running) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                byte[] data = new byte[packet.getLength()];
                System.arraycopy(buffer, 0, data, 0, packet.getLength());

                InetSocketAddress senderAddress = new InetSocketAddress(
                        packet.getAddress(),
                        Config.DEFAULT_PORT
                );

                SnakesProto.GameMessage message = SnakesProto.GameMessage.parseFrom(data);

                if (message.hasAnnouncement()) {
                    for (SnakesProto.GameAnnouncement game : message.getAnnouncement().getGamesList()) {
                        notifyListeners(game, senderAddress);
                    }
                }

            } catch (IOException e) {
                if (running) {
                    Logger.error("Multicast receive error: {}", e.getMessage());
                }
            }
        }
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
        String override = System.getProperty("snakes.multicast.if", System.getenv("SNAKES_MULTICAST_IF"));
        if (override != null && !override.isBlank()) {
            NetworkInterface forced = NetworkInterface.getByName(override.trim());
            if (forced != null && forced.isUp() && forced.supportsMulticast() && !forced.isLoopback() && hasIPv4Address(forced)) {
                return forced;
            } else {
                Logger.warn("Multicast override '{}' is unusable (up={}, multicast={}, loopback={}, ipv4={})",
                        override,
                        forced != null && forced.isUp(),
                        forced != null && forced.supportsMulticast(),
                        forced != null && forced.isLoopback(),
                        forced != null && hasIPv4Address(forced));
            }
        }

        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface iface = interfaces.nextElement();

            if (!iface.isUp() || !iface.supportsMulticast() || iface.isLoopback()) {
                continue;
            }

            if (!hasIPv4Address(iface)) {
                // macOS often has IPv6-only pseudo interfaces; skip them for IPv4 multicast
                continue;
            }

            return iface;
        }

        return null;
    }

    private boolean hasIPv4Address(NetworkInterface iface) throws SocketException {
        Enumeration<InetAddress> addresses = iface.getInetAddresses();
        while (addresses.hasMoreElements()) {
            InetAddress addr = addresses.nextElement();
            if (addr instanceof Inet4Address && !addr.isLoopbackAddress()) {
                return true;
            }
        }
        return false;
    }

    public void sendAnnouncement(SnakesProto.GameMessage announcement) {
        if (socket == null || socket.isClosed()) {
            return;
        }

        try {
            byte[] data = announcement.toByteArray();
            DatagramPacket packet = new DatagramPacket(data, data.length, group, multicastPort);
            socket.send(packet);
        } catch (IOException e) {
            Logger.error("Multicast send error: {}", e.getMessage());
        }
    }

    public record AnnouncementWithAddress(
            SnakesProto.GameAnnouncement announcement,
            InetSocketAddress senderAddress
    ) {}
}
