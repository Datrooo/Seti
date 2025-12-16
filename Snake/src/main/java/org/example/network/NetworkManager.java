package org.example.network;

import org.example.protocol.SnakesProto;
import org.example.util.Config;
import org.example.util.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
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

    /**
     * Запускает все сетевые компоненты
     */
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

        // Запускаем проверку таймаутов и ретрансмиссий
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

    /**
     * Останавливает все сетевые компоненты
     */
    public void stop() {
        running = false;

        transport.stop();
        discovery.stop();
        executorService.shutdownNow();

        Logger.info("NetworkManager stopped");
    }

    /**
     * Отправляет сообщение без требования подтверждения
     */
    public void send(SnakesProto.GameMessage message, InetSocketAddress destination) {
        byte[] data = message.toByteArray();
        transport.send(data, destination);
    }

    /**
     * Отправляет сообщение с требованием подтверждения
     */
    public void sendWithAck(SnakesProto.GameMessage message, InetSocketAddress destination) {
        ackManager.sendWithAck(message, destination);
    }

    /**
     * Отправляет ACK на полученное сообщение
     */
    public void sendAck(SnakesProto.GameMessage originalMessage, InetSocketAddress destination, int myPlayerId) {
        ackManager.sendAck(originalMessage, destination, myPlayerId);
    }

    /**
     * Регистрирует peer для отслеживания
     */
    public void registerPeer(InetSocketAddress address, int playerId) {
        ackManager.registerPeer(address, playerId);
    }

    /**
     * Удаляет peer
     */
    public void unregisterPeer(InetSocketAddress address) {
        ackManager.unregisterPeer(address);
    }

    /**
     * Обновляет активность peer'а
     */
    public void updatePeerActivity(InetSocketAddress address) {
        ackManager.updatePeerActivity(address);
    }

    /**
     * Отправляет multicast announcement
     */
    public void announceGame(SnakesProto.GameMessage announcement) {
        discovery.send(announcement); // ← было announce(), должно быть send()
    }

    /**
     * Проверяет таймауты peer'ов
     */
    public void checkPeerTimeouts(int timeoutMs, java.util.function.Consumer<PeerInfo> onTimeout) {
        ackManager.checkPeerTimeouts(timeoutMs, onTimeout);
    }

    // Делегирование методов диспетчера

    public MessageDispatcher getDispatcher() {
        return dispatcher;
    }

    public AckManager getAckManager() {
        return ackManager;
    }

    public UdpTransport getTransport() {
        return transport;
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * Подписка на объявления об играх
     */
    public void addAnnouncementListener(Consumer<MulticastDiscovery.AnnouncementWithAddress> listener) {
        discovery.addAnnouncementListener(listener);
    }

}
