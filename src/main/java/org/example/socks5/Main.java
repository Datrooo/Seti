package org.example.socks5;

public class Main {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: ... <port>");
            return;
        }

        int port;
        try {
            port = Integer.parseInt(args[0]);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Port out of range");
            }
        } catch (NumberFormatException e) {
            System.err.println("Invalid port: " + args[0]);
            return;
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return;
        }

        SocksProxyServer server;
        try {
            server = new SocksProxyServer(port);
        } catch (Exception e) {
            System.err.println("Could not start server on port: " + port);
            return;
        }

        SocksProxyServer finalServer = server;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook: stopping SOCKS5 proxy...");
            finalServer.stop();
        }));

        try {
            server.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
