import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class MulticastDiscovery {

    private static final int DEFAULT_PORT = 8888;
    private static final int SEND_INTERVAL_MS = 1000;
    private static final int PEER_TIMEOUT_MS = 5000;
    private static final String BEAT_PREFIX = "0";
    private static final int MAX_MESSAGE_SIZE = 1024;

    private final InetAddress group;
    private final int port;
    private final MulticastSocket socket;
    private final NetworkInterface netIf;
    private final SocketAddress groupSocketAddress;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    private final ConcurrentMap<String, PeerInfo> peers = new ConcurrentHashMap<>();

    private final String myId;
    private volatile boolean running = true;

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

    public MulticastDiscovery(String groupAddr, int port, String ifaceName) throws IOException {
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

        this.myId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);

        System.out.println(now() + " Launched id=" + myId + " iface=" +
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
        String payload = myId;
        String msg = BEAT_PREFIX + payload.length() + ":" + payload;
        byte[] data = msg.getBytes(StandardCharsets.UTF_8);

        if (data.length > MAX_MESSAGE_SIZE) {
            throw new IOException("Message too large: " + data.length);
        }

        DatagramPacket packet = new DatagramPacket(data, data.length, group, port);
        socket.send(packet);
    }


    private void handlePacket(DatagramPacket packet) {
        String received;
        try {
            received = new String(packet.getData(), packet.getOffset(),
                    packet.getLength(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            return;
        }

        if (received.isEmpty()) return;
        if (!received.startsWith(BEAT_PREFIX)) return;

        int colon = received.indexOf(':');
        if (colon < 0) return;

        int declaredLen;
        try {
            declaredLen = Integer.parseInt(received.substring(BEAT_PREFIX.length(), colon));
        } catch (NumberFormatException e) {
            return;
        }

        String payload = received.substring(colon + 1);
        if (payload.length() != declaredLen) {
            return;
        }

        String peerId = payload.trim();
        if (peerId.isEmpty() || peerId.equals(myId)) return;

        InetAddress senderAddr = packet.getAddress();
        long now = System.currentTimeMillis();

        PeerInfo prev = peers.putIfAbsent(peerId, new PeerInfo(peerId, senderAddr, now));
        if (prev == null) {
            System.out.println(now() + " New peer: " + senderAddr.getHostAddress() + " id=" + peerId);
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
            System.out.println(now() + " Peer timeout: id=" + id);
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
        if (args.length < 1 || args.length > 3) {
            System.out.println("Usage: java MulticastDiscovery <group> [port] [ifaceName]");
            return;
        }

        String group = args[0];
        int port = args.length >= 2 ? Integer.parseInt(args[1]) : DEFAULT_PORT;
        String iface = args.length == 3 ? args[2] : null;

        MulticastDiscovery multicastDiscovery;
        try {
            multicastDiscovery = new MulticastDiscovery(group, port, iface);
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
