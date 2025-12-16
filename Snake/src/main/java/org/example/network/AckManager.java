package org.example.network;

import org.example.game.serialization.MessageBuilder;
import org.example.protocol.SnakesProto;
import org.example.util.Config;
import org.example.util.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class AckManager {
    private final ConcurrentHashMap<InetSocketAddress, PeerInfo> peers;
    private final UdpTransport transport;
    private final int ackTimeoutMs;
    private final int maxRetries;

    public AckManager(UdpTransport transport) {
        this.peers = new ConcurrentHashMap<>();
        this.transport = transport;
        this.ackTimeoutMs = Config.ACK_TIMEOUT_MS;
        this.maxRetries = Config.MAX_RETRIES;
    }

    /**
     * Регистрирует peer для отслеживания
     */
    // AckManager.java
    public void registerPeer(InetSocketAddress address, int playerId) {
        peers.compute(address, (addr, existingRaw) -> {
            PeerInfo existing = (PeerInfo) existingRaw;

            if (existing == null) {
                Logger.debug("Registered peer: {} with id {}", addr, playerId);
                return new PeerInfo(addr, playerId);
            }

            int oldId = existing.getPlayerId();

            // не ухудшаем: если уже знаем id, не перетираем на 0
            if (playerId == 0 || oldId == playerId) {
                return existing;
            }

            // апгрейд 0 -> realId, сохраняя pendingMessages
            if (oldId == 0 && playerId != 0) {
                PeerInfo upgraded = new PeerInfo(addr, playerId);
                upgraded.getPendingMessages().putAll(existing.getPendingMessages());
                Logger.debug("Upgraded peer {} id {} -> {}", addr, oldId, playerId);
                return upgraded;
            }

            // иначе оставляем как есть
            return existing;
        });
    }


    /**
     * Удаляет peer
     */
    public void unregisterPeer(InetSocketAddress address) {
        peers.remove(address);
        Logger.debug("Unregistered peer: {}", address);
    }

    /**
     * Отправляет сообщение с требованием подтверждения
     */
    public void sendWithAck(SnakesProto.GameMessage message, InetSocketAddress destination) {
        PeerInfo peer = peers.get(destination);
        if (peer == null) {
            Logger.warn("Trying to send to unregistered peer: {}, auto-registering", destination);
            registerPeer(destination, 0);
            peer = peers.get(destination);
        }


        byte[] data = message.toByteArray();
        long msgSeq = message.getMsgSeq();

        // Сохраняем для возможной ретрансмиссии
        peer.addPendingMessage(msgSeq, data);

        // Отправляем
        transport.send(data, destination);

        Logger.debug("Sent message seq={} to {}, waiting for ACK", msgSeq, destination);
    }

    /**
     * Обрабатывает полученное AckMsg
     */
    public void handleAck(long msgSeq, InetSocketAddress sender) {
        PeerInfo peer = peers.get(sender);
        if (peer == null) {
            return;
        }

        peer.removePendingMessage(msgSeq);
        peer.updateActivity();

        Logger.debug("Received ACK for seq={} from {}", msgSeq, sender);
    }

    /**
     * Отправляет AckMsg в ответ на полученное сообщение
     */
    public void sendAck(SnakesProto.GameMessage originalMessage, InetSocketAddress destination, int myPlayerId) {
        if (!originalMessage.hasSenderId()) {
            return;
        }

        SnakesProto.GameMessage ack = MessageBuilder.createAck(
                originalMessage.getMsgSeq(),
                myPlayerId,
                originalMessage.getSenderId()
        );

        transport.send(ack.toByteArray(), destination);
        Logger.debug("Sent ACK for seq={} to {}", originalMessage.getMsgSeq(), destination);
    }

    /**
     * Проверяет таймауты и выполняет ретрансмиссии (вызывать периодически)
     */
    public void checkTimeouts() {
        for (PeerInfo peer : peers.values()) {
            ConcurrentHashMap<Long, PeerInfo.PendingMessage> pending = peer.getPendingMessages();

            for (Map.Entry<Long, PeerInfo.PendingMessage> entry : pending.entrySet()) {
                long msgSeq = entry.getKey();
                PeerInfo.PendingMessage pendingMsg = entry.getValue();

                if (pendingMsg.shouldRetry(ackTimeoutMs, maxRetries)) {
                    // Ретрансмиссия
                    pendingMsg.incrementRetry();
                    transport.send(pendingMsg.getData(), peer.getAddress());

                    Logger.warn("Retransmitting seq={} to {} (retry {})",
                            msgSeq, peer.getAddress(), pendingMsg.getRetryCount());
                } else if (pendingMsg.getRetryCount() >= maxRetries) {
                    // Превышен лимит попыток
                    pending.remove(msgSeq);
                    Logger.error("Message seq={} to {} failed after {} retries",
                            msgSeq, peer.getAddress(), maxRetries);
                }
            }
        }
    }

    /**
     * Проверяет таймауты всех peer'ов
     */
    public void checkPeerTimeouts(long timeoutMs, Consumer<PeerInfo> onTimeout) {
        Logger.debug("checkPeerTimeouts called with timeoutMs={}, peers count={}",
                timeoutMs, peers.size());

        List<PeerInfo> timedOutPeers = new ArrayList<>();

        for (PeerInfo peer : peers.values()) {
            if (peer.isTimedOut((int) timeoutMs)) {
                Logger.warn("Peer {} timed out", peer.getAddress());
                timedOutPeers.add(peer);
            }
        }

        // Вызываем callback для каждого таймаутнувшего peer
        for (PeerInfo peer : timedOutPeers) {
            onTimeout.accept(peer);
        }
    }



    /**
     * Обновляет время активности peer'а
     */
    public void updatePeerActivity(InetSocketAddress address) {
        PeerInfo peer = peers.get(address);
        if (peer != null) {
            peer.updateActivity(); // Обновляем timestamp
            Logger.debug("Updated activity for peer {}", address);
        } else {
            Logger.warn("Trying to update activity for unknown peer: {}", address);
        }
    }

    public PeerInfo getPeer(InetSocketAddress address) {
        return peers.get(address);
    }

    // ДОБАВЬТЕ ЭТОТ МЕТОД:
    public List<PeerInfo> getAllPeers() {
        return new ArrayList<>(peers.values());
    }

    public void redirectPeer(InetSocketAddress from, InetSocketAddress to) {
        if (from == null || to == null || from.equals(to)) return;

        PeerInfo fromPeer = (PeerInfo) peers.get(from);
        PeerInfo toPeer = (PeerInfo) peers.get(to);

        if (toPeer == null) {
            int pid = (fromPeer != null) ? fromPeer.getPlayerId() : 0;
            toPeer = new PeerInfo(to, pid);
            peers.put(to, toPeer);
        }

        if (fromPeer != null) {
            toPeer.getPendingMessages().putAll(fromPeer.getPendingMessages());
            peers.remove(from);
            Logger.info("Redirected pending messages {} -> {}", from, to);
        }
    }

}
