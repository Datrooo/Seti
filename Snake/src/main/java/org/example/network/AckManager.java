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

    public void registerPeer(InetSocketAddress address, int playerId) {
        peers.compute(address, (addr, existingRaw) -> {
            PeerInfo existing = (PeerInfo) existingRaw;

            if (existing == null) {
                Logger.info("[ACK-MGR] Registered NEW peer: {} with playerId={}", addr, playerId);
                return new PeerInfo(addr, playerId);
            }

            int oldId = existing.getPlayerId();

            if (playerId == 0 || oldId == playerId) {
                return existing;
            }

            if (oldId == 0 && playerId != 0) {
                PeerInfo upgraded = new PeerInfo(addr, playerId);
                upgraded.getPendingMessages().putAll(existing.getPendingMessages());
                Logger.debug("Upgraded peer {} id {} -> {}", addr, oldId, playerId);
                return upgraded;
            }

            return existing;
        });
    }

    public void unregisterPeer(InetSocketAddress address) {
        peers.remove(address);
        Logger.debug("Unregistered peer: {}", address);
    }

    public void sendWithAck(SnakesProto.GameMessage message, InetSocketAddress destination) {
        PeerInfo peer = peers.get(destination);
        if (peer == null) {
            Logger.warn("Trying to send to unregistered peer: {}, auto-registering", destination);
            registerPeer(destination, 0);
            peer = peers.get(destination);
        }

        byte[] data = message.toByteArray();
        long msgSeq = message.getMsgSeq();
        peer.addPendingMessage(msgSeq, data);
        transport.send(data, destination);
        Logger.info("[ACK-MGR] Sent message seq={} to {}, waiting for ACK", msgSeq, destination);
    }

    public void handleAck(long msgSeq, InetSocketAddress sender) { // Обработка полученного ack
        PeerInfo peer = peers.get(sender);
        if (peer == null) {
            return;
        }
        peer.removePendingMessage(msgSeq);
        peer.updateActivity();
        Logger.info("[ACK-MGR] ✓ Received ACK for seq={} from {}", msgSeq, sender);
    }

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
        Logger.info("[ACK-MGR] Sent ACK for seq={} to {} (receiver_id={})", 
                originalMessage.getMsgSeq(), destination, originalMessage.getSenderId());
    }

    public void checkTimeouts() {
        for (PeerInfo peer : peers.values()) {
            ConcurrentHashMap<Long, PeerInfo.PendingMessage> pending = peer.getPendingMessages();

            for (Map.Entry<Long, PeerInfo.PendingMessage> entry : pending.entrySet()) {
                long msgSeq = entry.getKey();
                PeerInfo.PendingMessage pendingMsg = entry.getValue();

                if (pendingMsg.shouldRetry(ackTimeoutMs, maxRetries)) {
                    pendingMsg.incrementRetry();
                    transport.send(pendingMsg.getData(), peer.getAddress());

                    Logger.warn("[ACK-MGR] ⟳ Retransmitting seq={} to {} (retry {}/{})",
                            msgSeq, peer.getAddress(), pendingMsg.getRetryCount(), maxRetries);
                } else if (pendingMsg.getRetryCount() >= maxRetries) {
                    pending.remove(msgSeq);
                    Logger.error("[ACK-MGR] ✗ Message seq={} to {} FAILED after {} retries",
                            msgSeq, peer.getAddress(), maxRetries);
                }
            }
        }
    }

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

        for (PeerInfo peer : timedOutPeers) {
            onTimeout.accept(peer);
        }
    }

    public void updatePeerActivity(InetSocketAddress address) {
        PeerInfo peer = peers.get(address);
        if (peer != null) {
            peer.updateActivity();
            Logger.debug("Updated activity for peer {}", address);
        } else {
            Logger.warn("Trying to update activity for unknown peer: {}", address);
        }
    }

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
