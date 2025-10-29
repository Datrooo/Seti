package org.example;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalTime;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Server {

    private final int port;
    private final Path uploadsDir;
    private final ExecutorService clientPool;
    private final ScheduledExecutorService statsScheduler;
    private final ConcurrentHashMap<SocketAddress, ClientStats> activeClients = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ServerSocket serverSocket;

    public Server(int port, int maxClients) throws IOException {
        this.port = port;
        this.uploadsDir = Paths.get("uploads").toAbsolutePath().normalize();
        Files.createDirectories(uploadsDir);

        this.clientPool = Executors.newFixedThreadPool(maxClients);
        this.statsScheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            System.out.println(time() + " Server already running");
            return;
        }

        statsScheduler.scheduleAtFixedRate(this::printAllStats, 3, 3, TimeUnit.SECONDS);

        try (ServerSocket ss = new ServerSocket(port)) {
            serverSocket = ss;
            ss.setReuseAddress(true);
            ss.setSoTimeout(1000);

            System.out.println(time() + " Server started on port " + port);

            while (running.get()) {
                try {
                    Socket client = ss.accept();
                    clientPool.execute(new ClientHandler(client, uploadsDir, this));
                } catch (SocketTimeoutException ignore) {}
            }
        } catch (IOException e) {
            if (running.get()) {
                System.err.println(time() + " Server I/O error: " + e.getMessage());
            }
        } finally {
            stop();
            System.out.println(time() + " Server shut down.");
        }
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignore) {}

        statsScheduler.shutdownNow();
        clientPool.shutdownNow();
    }

    void registerClient(SocketAddress addr, ClientStats stats) {
        activeClients.put(addr, stats);
    }

    void unregisterClient(SocketAddress addr) {
        if (activeClients.containsKey(addr)) {
            printStatsFor(addr, activeClients.get(addr));
        }
        activeClients.remove(addr);
    }

    private void printAllStats() {
        if (activeClients.isEmpty()) return;
        System.out.println("\n==== " + time() + " ACTIVE CONNECTIONS ====");
        for (Map.Entry<SocketAddress, ClientStats> entry : activeClients.entrySet()) {
            printStatsFor(entry.getKey(), entry.getValue());
        }
        System.out.println("===========================================\n");
    }

    private void printStatsFor(SocketAddress addr, ClientStats stats) {
        long now = System.nanoTime();
        long total = stats.totalBytes.get();
        long recent = stats.bytesSinceLast.getAndSet(0);
        long intervalNanos = Math.max(1, now - stats.lastTickNanos);
        stats.lastTickNanos = now;

        double elapsedSec = Math.max(1e-9, (now - stats.startTime) / 1e9);
        double avg = total / elapsedSec;
        double inst = recent / (intervalNanos / 1e9);

        System.out.printf("%s %s | instant=%.2f B/s | average=%.2f B/s | total=%d bytes%n",
                time(), addr, inst, avg, total);
    }

    static String time() {
        return "[" + LocalTime.now().withNano(0) + "]";
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java Server <port> <maxClients>");
            return;
        }
        int port = Integer.parseInt(args[0]);
        int maxClients = Integer.parseInt(args[1]);

        Server server;
        try {
            server = new Server(port, maxClients);
        } catch (IOException e) {
            System.err.println(time() + " Init failed: " + e.getMessage());
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));

        server.start();
    }

    static class ClientStats {
        final long startTime = System.nanoTime();
        final AtomicLong totalBytes = new AtomicLong();
        final AtomicLong bytesSinceLast = new AtomicLong();
        volatile long lastTickNanos = startTime;
    }

    static class ClientHandler implements Runnable {
        private final Socket socket;
        private final Path uploadsDir;
        private final Server server;

        ClientHandler(Socket socket, Path uploadsDir, Server server) {
            this.socket = socket;
            this.uploadsDir = uploadsDir;
            this.server = server;
        }

        @Override
        public void run() {
            SocketAddress addr = socket.getRemoteSocketAddress();
            ClientStats stats = new ClientStats();
            server.registerClient(addr, stats);

            try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                 DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                 socket) {

                int nameLen = in.readInt();
                if (nameLen <= 0 || nameLen > 4096) throw new IllegalArgumentException("Invalid filename length");

                byte[] nameBytes = new byte[nameLen];
                in.readFully(nameBytes);
                String filename = new String(nameBytes, StandardCharsets.UTF_8);

                long declaredSize = in.readLong();
                if (declaredSize < 0 || declaredSize > (1L << 40))
                    throw new IllegalArgumentException("Invalid file size");

                String safeName = Paths.get(filename).getFileName().toString();
                Path finalPath = uploadsDir.resolve(safeName).normalize();
                if (!finalPath.startsWith(uploadsDir))
                    throw new IOException("Unsafe filename path");

                String prefix = (safeName + ".").replaceAll("[/\\\\]", ".");
                if (prefix.length() < 3) prefix = "up_";

                Path tmp = Files.createTempFile(uploadsDir, prefix, ".part");
                boolean success = false;
                try {
                    System.out.println(Server.time() + " Receiving file '" + safeName + "' from " + addr);

                    try (OutputStream fout = new BufferedOutputStream(Files.newOutputStream(tmp))) {
                        long received = 0;
                        byte[] buf = new byte[64 * 1024];
                        while (received < declaredSize) {
                            int toRead = (int) Math.min(buf.length, declaredSize - received);
                            int r = in.read(buf, 0, toRead);
                            if (r == -1) throw new EOFException("Client disconnected");
                            fout.write(buf, 0, r);
                            received += r;
                            stats.totalBytes.addAndGet(r);
                            stats.bytesSinceLast.addAndGet(r);
                        }
                    }

                    try {
                        Files.move(tmp, finalPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                    } catch (AtomicMoveNotSupportedException e) {
                        Files.move(tmp, finalPath, StandardCopyOption.REPLACE_EXISTING);
                    }

                    System.out.println(Server.time() + " File '" + safeName + "' received successfully from " + addr);
                    out.flush();
                    success = true;
                } finally {
                    if (!success) {
                        try { Files.deleteIfExists(tmp); } catch (IOException ignored) {}
                    }
                }

            } catch (IOException e) {
                System.err.println(Server.time() + " " + addr + " error: " + e.getMessage());
            } finally {
                server.unregisterClient(addr);
                try { socket.close(); } catch (IOException ignored) {}
            }
        }
    }
}
