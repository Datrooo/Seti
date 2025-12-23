package org.example.network;

import org.example.util.Logger;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class UdpTransport {
    private final int port;
    private DatagramChannel channel;
    private Selector selector;
    private final BlockingQueue<ReceivedPacket> receivedPackets;
    private volatile boolean running;
    private int localPort;

    public UdpTransport(int port) {
        this.port = port;
        this.localPort = -1;
        this.receivedPackets = new LinkedBlockingQueue<>();
        this.running = false;
    }

    public void start() throws IOException {
        channel = DatagramChannel.open();
        channel.configureBlocking(false);
        channel.socket().bind(new InetSocketAddress(port));
        localPort = channel.socket().getLocalPort();

        selector = Selector.open();
        channel.register(selector, SelectionKey.OP_READ);

        running = true;

        Logger.info("UDP Transport started on port {}", localPort);
    }

    public void send(byte[] data, InetSocketAddress destination) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            channel.send(buffer, destination);
            Logger.info("[UDP-SEND] {} bytes to {} (local port: {})", data.length, destination, localPort);
        } catch (IOException e) {
            Logger.error("Failed to send packet to {}: {}", destination, e.getMessage());
        }
    }

    public ReceivedPacket receive() throws InterruptedException {
        return receivedPackets.take();
    }

    public void receiveLoop() {
        ByteBuffer buffer = ByteBuffer.allocate(65536);

        while (running) {
            try {
                int ready = selector.select(100);

                if (ready == 0) {
                    continue;
                }

                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (key.isReadable()) {
                        buffer.clear();
                        InetSocketAddress sender = (InetSocketAddress) channel.receive(buffer);

                        if (sender != null) {
                            buffer.flip();
                            byte[] data = new byte[buffer.remaining()];
                            buffer.get(data);

                            receivedPackets.offer(new ReceivedPacket(data, sender));
                            Logger.info("[UDP-RECV] {} bytes from {} (local port: {})", data.length, sender, localPort);
                        }
                    }
                }
            } catch (IOException e) {
                if (running) {
                    Logger.error("Error in receive loop: {}", e.getMessage());
                }
            }
        }
    }

    public void stop() {
        running = false;

        try {
            if (selector != null) {
                selector.close();
            }
            if (channel != null) {
                channel.close();
            }
            Logger.info("UDP Transport stopped");
        } catch (IOException e) {
            Logger.error("Error stopping UDP transport: {}", e.getMessage());
        }
    }

    public int getPort() {
        return localPort;
    }

    public record ReceivedPacket(byte[] data, InetSocketAddress sender) {}
}
