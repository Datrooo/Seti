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
        group = InetAddress.getByName(multicastAddress);

        try {
            NetworkInterface networkInterface = getNetworkInterface();
            socket.joinGroup(new InetSocketAddress(group, multicastPort), networkInterface);
        } catch (Exception e) {
            socket.joinGroup(group);
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
                    socket.leaveGroup(new InetSocketAddress(group, multicastPort), networkInterface);
                } catch (Exception e) {
                    socket.leaveGroup(group);
                }
                socket.close();
            }
            Logger.info("Multicast discovery stopped");
        } catch (IOException e) {
            Logger.error("Error stopping multicast discovery: {}", e.getMessage());
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
                    for (SnakesProto.GameAnnouncement game :
                            message.getAnnouncement().getGamesList()) {
                        notifyListeners(game, senderAddress);
                    }
                }

            } catch (IOException e) {
                if (running) {
                    Logger.error("Error receiving multicast: {}", e.getMessage());
                }
            }
        }
    }

    public void send(SnakesProto.GameMessage message) {
        if (socket == null || !running) {
            return;
        }

        try {
            byte[] data = message.toByteArray();
            DatagramPacket packet = new DatagramPacket(
                    data,
                    data.length,
                    group,
                    multicastPort
            );
            socket.send(packet);
            Logger.debug("Sent multicast announcement");
        } catch (IOException e) {
            Logger.error("Failed to send multicast: {}", e.getMessage());
        }
    }

    public void addAnnouncementListener(Consumer<AnnouncementWithAddress> listener) {
        listeners.add(listener);
    }

    public void removeAnnouncementListener(Consumer<AnnouncementWithAddress> listener) {
        listeners.remove(listener);
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
        while (interfaces.hasMoreElements()) {
            NetworkInterface iface = interfaces.nextElement();
            if (!iface.isLoopback() && iface.isUp() && iface.supportsMulticast()) {
                return iface;
            }
        }
        return NetworkInterface.getByInetAddress(InetAddress.getLoopbackAddress());
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
            Logger.debug("Sent multicast announcement");

        } catch (IOException e) {
            Logger.error("Error sending multicast announcement: {}", e.getMessage());
        }
    }


    public record AnnouncementWithAddress(
            SnakesProto.GameAnnouncement announcement,
            InetSocketAddress senderAddress
    ) {}
}
