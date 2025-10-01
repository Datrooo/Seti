package org.example;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MulticastDiscovery {

    private static final int DEFAULT_PORT = 5000;
    private static final int SEND_INTERVAL_MS = 3000;
    private static final int PEER_TIMEOUT_MS = 10000;
    private static final int MAX_PAYLOAD = 500;
    private static final int HEADER_SIZE = 1 + 2;
    private static final int MAX_MESSAGE_SIZE = HEADER_SIZE + MAX_PAYLOAD;

    private final InetAddress group;
    private final int port;
    private final MulticastSocket socket;
    private final NetworkInterface netIf;
    private final SocketAddress groupSocketAddress;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    private final ConcurrentMap<String, PeerInfo> peers = new ConcurrentHashMap<>();

    private final String myMessage;
    private volatile boolean running = true;
    private final AtomicBoolean goodbyeSent = new AtomicBoolean(false);

    private static class PeerInfo {
        final String id;
        volatile InetAddress address;
        volatile long lastSeen;

        PeerInfo(String id, InetAddress address, long lastSeen) {
            this.id = id;
            this.address = address;
            this.lastSeen = lastSeen;
        }
    }

    public MulticastDiscovery(String groupAddr, int port, String ifaceName, String myMessage) throws IOException {
        this.group = InetAddress.getByName(groupAddr);
        this.port = port;
        this.groupSocketAddress = new InetSocketAddress(group, port);

        this.netIf = pickNetworkInterface(group, ifaceName);
        if (this.netIf == null) {
            throw new IOException("Can't find suitable network interface for group.");
        }

        this.socket = new MulticastSocket(port);

        try {
            this.socket.setNetworkInterface(netIf);
        } catch (SocketException e) {
            System.err.println(now() + " Can't set network interface: " + e.getMessage());
        }

        try {
            this.socket.setTimeToLive(1);
        } catch (IOException ignored) {
        }

        this.socket.joinGroup(groupSocketAddress, netIf);

        if (myMessage == null || myMessage.isEmpty()) {
            throw new IllegalArgumentException("Message cannot be null or empty");
        }
        this.myMessage = myMessage;

        System.out.println(now() + " Launched message=\"" + myMessage + "\" iface=" +
                netIf.getName() + " group=" + group.getHostAddress() + " port=" + port);
    }

    private NetworkInterface pickNetworkInterface(InetAddress groupAddr, String ifaceName) throws SocketException {
        if (ifaceName != null && !ifaceName.isEmpty()) {
            try {
                NetworkInterface byName = NetworkInterface.getByName(ifaceName);
                if (byName != null && byName.isUp() && !byName.isLoopback() && byName.supportsMulticast()) {
                    return byName;
                } else {
                    System.err.println(now() + " Interface " + ifaceName + " not found or unsuitable.");
                }
            } catch (SocketException se) {
                System.err.println(now() + " Failed to pick interface " + ifaceName + ": " + se.getMessage());
            }
        }

        boolean needIpv6 = groupAddr instanceof Inet6Address;
        Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
        while (en.hasMoreElements()) {
            NetworkInterface nif = en.nextElement();
            try {
                if (!nif.isUp() || nif.isLoopback() || !nif.supportsMulticast()) continue;
            } catch (SocketException ex) {
                continue;
            }

            Enumeration<InetAddress> addrs = nif.getInetAddresses();
            while (addrs.hasMoreElements()) {
                InetAddress a = addrs.nextElement();
                if (needIpv6 && a instanceof Inet6Address) return nif;
                if (!needIpv6 && a instanceof Inet4Address) return nif;
            }
        }

        if (needIpv6) {
            throw new SocketException("Can't find IPv6 interface for multicast group " + groupAddr);
        } else {
            throw new SocketException("Can't find IPv4 interface for multicast group " + groupAddr);
        }
    }

    public void start() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                sendPresence();
            } catch (IOException e) {
                System.out.println(now() + " Failed to send presence: " + e.getMessage());
            }
        }, 0, SEND_INTERVAL_MS, TimeUnit.MILLISECONDS);

        scheduler.execute(() -> {
            while (running) {
                byte[] buf = new byte[MAX_MESSAGE_SIZE];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                try {
                    socket.receive(packet);
                    handlePacket(packet);
                } catch (IOException e) {
                    if (running) {
                        System.err.println(now() + " Failed to receive packet: " + e.getMessage());
                    }
                }
            }
        });

        scheduler.scheduleAtFixedRate(this::cleanupPeers, PEER_TIMEOUT_MS, PEER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void sendPresence() throws IOException {
        sendPresenceWithPrefix((byte)0);
    }

    private void sendPresenceWithPrefix(byte prefix) throws IOException {
        byte[] payload = myMessage.getBytes(StandardCharsets.UTF_8);
        if (payload.length > MAX_PAYLOAD) {
            throw new IOException("Payload too large: " + payload.length);
        }

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeByte(prefix);
        out.writeShort(payload.length);
        out.write(payload);

        byte[] data = out.toByteArray();
        if (data.length > MAX_MESSAGE_SIZE) {
            throw new IOException("Message too large after encoding: " + data.length);
        }

        DatagramPacket packet = new DatagramPacket(data, data.length, group, port);
        socket.send(packet);

        if (prefix == 1) {
            System.out.println(now() + " Sent goodbye message=\"" + myMessage + "\"");
        }
    }

    private void sendGoodbye() {
        if (!goodbyeSent.compareAndSet(false, true)) return;

        try {
            sendPresenceWithPrefix((byte)1);
            try {
                Thread.sleep(150);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        } catch (IOException e) {
            System.err.println(now() + " Failed to send goodbye: " + e.getMessage());
        }
    }

    private void handlePacket(DatagramPacket packet) {
        byte[] data = packet.getData();
        int offset = packet.getOffset();
        int length = packet.getLength();

        if (length < HEADER_SIZE) return;

        byte prefix = data[offset];
        if (prefix != 0 && prefix != 1) return;

        int declaredLen = ((data[offset + 1] & 0xFF) << 8) | (data[offset + 2] & 0xFF);
        if (declaredLen > MAX_PAYLOAD) return;

        if (length != HEADER_SIZE + declaredLen) return;

        String payload;
        try {
            payload = new String(data, offset + HEADER_SIZE, declaredLen, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return;
        }

        String peerMsg = payload.trim();
        if (peerMsg.isEmpty() || peerMsg.equals(myMessage)) return;

        InetAddress senderAddr = packet.getAddress();
        long now = System.currentTimeMillis();

        if (prefix == 1) {
            PeerInfo removed = peers.remove(peerMsg);
            if (removed != null) {
                System.out.println(now() + " Peer left (goodbye): " + senderAddr.getHostAddress() + " msg=\"" + peerMsg + "\"");
                printAlivePeers();
            }
            return;
        }

        PeerInfo prev = peers.putIfAbsent(peerMsg, new PeerInfo(peerMsg, senderAddr, now));
        if (prev == null) {
            System.out.println(now() + " New peer: " + senderAddr.getHostAddress() + " msg=\"" + peerMsg + "\"");
            printAlivePeers();
        } else {
            prev.address = senderAddr;
            prev.lastSeen = now;
        }
    }

    private void cleanupPeers() {
        long now = System.currentTimeMillis();
        List<String> removed = new ArrayList<>();

        for (Map.Entry<String, PeerInfo> entry : peers.entrySet()) {
            if (now - entry.getValue().lastSeen > PEER_TIMEOUT_MS) {
                if (peers.remove(entry.getKey(), entry.getValue())) {
                    removed.add(entry.getKey());
                }
            }
        }

        for (String id : removed) {
            System.out.println(now() + " Peer timeout: msg=\"" + id + "\"");
        }

        if (!removed.isEmpty()) {
            printAlivePeers();
        }
    }

    private void printAlivePeers() {
        System.out.println(now() + " Active peers (" + peers.size() + "): " + peers.keySet());
    }

    private String now() {
        return new SimpleDateFormat("HH:mm:ss").format(new Date());
    }

    public void stop() {
        running = false;

        sendGoodbye();

        scheduler.shutdownNow();

        try {
            socket.leaveGroup(groupSocketAddress, netIf);
        } catch (IOException e) {
            System.out.println(now() + " Error leaving group: " + e.getMessage());
        }
        socket.close();
        System.out.println(now() + " Program stopped");
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 4) {
            System.out.println("Usage: java MulticastDiscovery <group> <message> [port] [ifaceName]");
            return;
        }

        String group = args[0];
        String message = args[1];
        int port = args.length >= 3 ? Integer.parseInt(args[2]) : DEFAULT_PORT;
        String iface = args.length == 4 ? args[3] : null;

        MulticastDiscovery multicastDiscovery;
        try {
            multicastDiscovery = new MulticastDiscovery(group, port, iface, message);
        } catch (IOException e) {
            System.err.println("Failed to initialize application: " + e.getMessage());
            return;
        }

        multicastDiscovery.start();

        Runtime.getRuntime().addShutdownHook(new Thread(multicastDiscovery::stop));

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
