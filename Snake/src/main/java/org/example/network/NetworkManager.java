package org.example.network;

import org.example.protocol.SnakesProto;
import org.example.util.Config;
import org.example.util.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class NetworkManager {
    private final UdpTransport transport;
    private final MulticastDiscovery discovery;
    private final AckManager ackManager;
    private final MessageDispatcher dispatcher;
    private final ExecutorService executorService;
    private volatile boolean running;

    public NetworkManager(int port) {
        this.transport = new UdpTransport(port);
        this.discovery = new MulticastDiscovery();
        this.ackManager = new AckManager(transport);
        this.dispatcher = new MessageDispatcher();
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
        this.running = false;
    }

    public void start() throws IOException {
        transport.start();
        discovery.start();
        running = true;

        // Запускаем приемник UDP пакетов
        executorService.submit(() -> {
            transport.receiveLoop();
        });

        // Запускаем обработчик входящих пакетов
        executorService.submit(() -> {
            while (running) {
                try {
                    UdpTransport.ReceivedPacket packet = transport.receive();
                    dispatcher.dispatch(packet.data(), packet.sender());
                } catch (InterruptedException e) {
                    if (running) {
                        Logger.error("Network receive interrupted: {}", e.getMessage());
                    }
                    break;
                }
            }
        });

        // Запускаем проверку таймаутов
        executorService.submit(() -> {
            while (running) {
                try {
                    Thread.sleep(Config.ACK_TIMEOUT_MS / 2);
                    ackManager.checkTimeouts();
                } catch (InterruptedException e) {
                    if (running) {
                        Logger.error("Timeout checker interrupted: {}", e.getMessage());
                    }
                    break;
                }
            }
        });

        // Регистрируем обработчик ACK сообщений
        dispatcher.onAck((message, sender) -> {
            ackManager.handleAck(message.getMsgSeq(), sender);
        });

        Logger.info("NetworkManager started successfully");
    }

    public void stop() {
        running = false;

        transport.stop();
        discovery.stop();
        executorService.shutdownNow();

        Logger.info("NetworkManager stopped");
    }

    public void send(SnakesProto.GameMessage message, InetSocketAddress destination) {
        byte[] data = message.toByteArray();
        transport.send(data, destination);
    }

    public void sendWithAck(SnakesProto.GameMessage message, InetSocketAddress destination) {
        ackManager.sendWithAck(message, destination);
    }

    public void sendAck(SnakesProto.GameMessage originalMessage, InetSocketAddress destination, int myPlayerId) {
        ackManager.sendAck(originalMessage, destination, myPlayerId);
    }

    public void registerPeer(InetSocketAddress address, int playerId) {
        ackManager.registerPeer(address, playerId);
        ackManager.updatePeerActivity(address);
    }

    public void unregisterPeer(InetSocketAddress address) {
        ackManager.unregisterPeer(address);
    }

    public void updatePeerActivity(InetSocketAddress address) {
        ackManager.updatePeerActivity(address);
    }

    public void checkPeerTimeouts(long timeoutMs, Consumer<PeerInfo> onTimeout) {
        ackManager.checkPeerTimeouts(timeoutMs, onTimeout);
    }

    public List<PeerInfo> getAllPeers() {
        return ackManager.getAllPeers();
    }

    public void redirectPeer(InetSocketAddress from, InetSocketAddress to, int toPlayerId) {
        if (to == null) return;
        registerPeer(to, toPlayerId);
        ackManager.redirectPeer(from, to);
    }

    public MessageDispatcher getDispatcher() {
        return dispatcher;
    }

    public void announceGame(SnakesProto.GameMessage announcement) {
        if (discovery != null) {
            discovery.sendAnnouncement(announcement);
        } else {
            Logger.warn("MulticastDiscovery not initialized, cannot announce game");
        }
    }


}
