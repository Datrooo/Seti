package org.example;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalTime;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Server {

    private final int port;
    private final Path uploadsDir;
    private final ExecutorService clientPool;
    private final ScheduledExecutorService statsScheduler;
    private final ConcurrentHashMap<SocketAddress, ClientStats> activeClients = new ConcurrentHashMap<>();

    public Server(int port, int maxClients) throws IOException {
        this.port = port;
        this.uploadsDir = Paths.get("uploads").toAbsolutePath().normalize();
        Files.createDirectories(uploadsDir);

        this.clientPool = Executors.newFixedThreadPool(maxClients);
        this.statsScheduler = Executors.newSingleThreadScheduledExecutor();
        this.statsScheduler.scheduleAtFixedRate(this::printAllStats, 3, 3, TimeUnit.SECONDS);
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println(time() + " Server started on port " + port);
            while (true) {
                Socket client = serverSocket.accept();
                clientPool.submit(new ClientHandler(client, uploadsDir, this));
            }
        } finally {
            clientPool.shutdown();
            statsScheduler.shutdown();
            System.out.println(time() + " Server shut down.");
        }
    }

    void registerClient(SocketAddress addr, ClientStats stats) {
        activeClients.put(addr, stats);
    }

    void unregisterClient(SocketAddress addr, boolean printFinal) {
        if (printFinal && activeClients.containsKey(addr)) {
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

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: java Server <port> <maxClients>");
            return;
        }
        int port = Integer.parseInt(args[0]);
        int maxClients = Integer.parseInt(args[1]);
        new Server(port, maxClients).start();
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

            Path tmp = null;
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                 DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

                int nameLen = in.readInt();
                if (nameLen <= 0 || nameLen > 4096) throw new IOException("Invalid filename length");

                byte[] nameBytes = new byte[nameLen];
                in.readFully(nameBytes);
                String filename = new String(nameBytes, StandardCharsets.UTF_8);

                long declaredSize = in.readLong();
                if (declaredSize < 0 || declaredSize > (1L << 40))
                    throw new IOException("Invalid file size");

                String safeName = Paths.get(filename).getFileName().toString();
                Path finalPath = uploadsDir.resolve(safeName).normalize();
                if (!finalPath.startsWith(uploadsDir))
                    throw new IOException("Unsafe filename path");

                String prefix = (safeName + ".").replaceAll("[/\\\\]", ".");
                if (prefix.length() < 3) prefix = "up_";
                tmp = Files.createTempFile(uploadsDir, prefix, ".part");

                System.out.println(Server.time() + " Receiving file '" + safeName + "' from " + addr);

                long received = 0;
                try (OutputStream fout = new BufferedOutputStream(Files.newOutputStream(tmp))) {
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
                tmp = null;

                System.out.println(Server.time() + " File '" + safeName + "' received successfully from " + addr);

                if (Files.size(finalPath) == declaredSize) out.writeByte(1);
                else {
                    System.err.println(Server.time() + " Size mismatch for '" + safeName + "'");
                    out.writeByte(0);
                }
                out.flush();

            } catch (IOException e) {
                System.err.println(Server.time() + " " + addr + " error: " + e.getMessage());
                if (tmp != null) try { Files.deleteIfExists(tmp); } catch (IOException ignored) {}
            } finally {
                server.unregisterClient(addr, true);
                try { socket.close(); } catch (IOException ignored) {}
            }
        }
    }
}
