package org.example;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

public class Client {
    
    public static void main(String[] args) throws Exception {
        ClientConfig config = parseArguments(args);
        if (config == null) {
            return;
        }
        
        transferFile(config);
    }
    
    private static ClientConfig parseArguments(String[] args) {
        if (args.length < 3) {
            System.out.println("args = <serverHost> <port> <filePath>");
            return null;
        }

        String host = args[0];
        int port;
        try {
            port = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println("Invalid port number: " + args[1]);
            return null;
        }
        
        Path filePath = Paths.get(args[2]);

        if (!validateFile(filePath)) {
            return null;
        }
        
        return new ClientConfig(host, port, filePath);
    }
    
    private static boolean validateFile(Path filePath) {
        if (!Files.exists(filePath) || !Files.isRegularFile(filePath)) {
            System.err.println("File not found: " + filePath.toAbsolutePath());
            return false;
        }

        String fileName = filePath.getFileName().toString();
        long fileSize;
        
        try {
            fileSize = Files.size(filePath);
        } catch (IOException e) {
            System.err.println("Cannot determine file size: " + e.getMessage());
            return false;
        }
        
        byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        if (nameBytes.length == 0 || nameBytes.length > 4096) {
            System.err.println("Filename length must be 1..4096 bytes in UTF-8");
            return false;
        }
        
        if (fileSize < 0 || fileSize > (1L << 40)) {
            System.err.println("File size must be 0..1TB");
            return false;
        }
        
        return true;
    }
    
    private static void transferFile(ClientConfig config) {
        System.out.println("[Client] Connecting to " + config.host + ":" + config.port);
        
        try (Socket socket = createSocket(config);
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
             DataInputStream in = new DataInputStream(socket.getInputStream());
             InputStream fin = new BufferedInputStream(Files.newInputStream(config.filePath))) {
            
            sendFileHeader(out, config);
            sendFileData(out, fin, config.filePath);
            waitForConfirmation(in);
            
        } catch (IOException e) {
            System.err.println("[Client] Error: " + e.getMessage());
        }
    }
    
    private static Socket createSocket(ClientConfig config) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(config.host, config.port), 5000);
        return socket;
    }
    
    private static void sendFileHeader(DataOutputStream out, ClientConfig config) throws IOException {
        String fileName = config.filePath.getFileName().toString();
        long fileSize = Files.size(config.filePath);
        
        byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        out.writeInt(nameBytes.length);
        out.write(nameBytes);
        out.writeLong(fileSize);
    }
    
    private static void sendFileData(DataOutputStream out, InputStream fin, Path filePath) throws IOException {
        long fileSize = Files.size(filePath);
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
                printProgress(sent, fileSize);
                lastPrint = now;
            }
        }

        out.flush();
        System.out.println("[Client] Progress: 100.00%");
        System.out.println("[Client] File sent. Waiting for confirmation...");
    }
    
    private static void printProgress(long sent, long fileSize) {
        double pct = fileSize == 0 ? 100.0 : (100.0 * sent / Math.max(1, fileSize));
        System.out.printf("[Client] Progress: %.2f%%%n", Math.min(100.0, pct));
    }
    
    private static void waitForConfirmation(DataInputStream in) throws IOException {
        int result = in.readByte();
        if (result == 1)
            System.out.println("[Client] File transferred successfully.");
        else
            System.out.println("[Client] File transfer failed.");
    }
    
    private static class ClientConfig {
        final String host;
        final int port;
        final Path filePath;
        
        ClientConfig(String host, int port, Path filePath) {
            this.host = host;
            this.port = port;
            this.filePath = filePath;
        }
    }
}
