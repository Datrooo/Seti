package org.example;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

public class Client {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("args = <serverHost> <port> <filePath>");
            return;
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        Path filePath = Paths.get(args[2]);

        if (!Files.exists(filePath) || !Files.isRegularFile(filePath)) {
            System.err.println("File not found: " + filePath.toAbsolutePath());
            return;
        }

        String fileName = filePath.getFileName().toString();
        long fileSize = Files.size(filePath);

        byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        if (nameBytes.length == 0 || nameBytes.length > 4096) {
            System.err.println("Filename length must be 1..4096 bytes in UTF-8");
            return;
        }
        if (fileSize < 0 || fileSize > (1L << 40)) {
            System.err.println("File size must be 0..1TB");
            return;
        }

        System.out.println("[Client] Connecting to " + host + ":" + port);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 5000);
            socket.setTcpNoDelay(true);

            try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                 DataInputStream in = new DataInputStream(socket.getInputStream());
                 InputStream fin = new BufferedInputStream(Files.newInputStream(filePath))) {

                out.writeInt(nameBytes.length);
                out.write(nameBytes);
                out.writeLong(fileSize);

                byte[] buf = new byte[64 * 1024];
                long sent = 0;
                long lastPrint = System.currentTimeMillis();

                while (sent < fileSize) {
                    int r = fin.read(buf);
                    if (r == -1) break;
                    out.write(buf, 0, r);
                    sent += r;

                    long now = System.currentTimeMillis();
                    if (now - lastPrint >= 1000) {
                        double pct = fileSize == 0 ? 100.0 : (100.0 * sent / Math.max(1, fileSize));
                        System.out.printf("[Client] Progress: %.2f%%%n", Math.min(100.0, pct));
                        lastPrint = now;
                    }
                }

                out.flush();
                System.out.println("[Client] Progress: 100.00%");
                System.out.println("[Client] File sent. Waiting for confirmation...");

                int result = in.readByte();
                if (result == 1)
                    System.out.println("[Client] File transferred successfully.");
                else
                    System.out.println("[Client] File transfer failed.");
            }
        } catch (IOException e) {
            System.err.println("[Client] Error: " + e.getMessage());
        }
    }
}
