package org.example.network;

import org.example.util.Logger;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

public class PeerInfo {
    private final InetSocketAddress address;
    private final int playerId;
    private long lastActivityTime;
    private final ConcurrentHashMap<Long, PendingMessage> pendingMessages;

    public PeerInfo(InetSocketAddress address, int playerId) {
        this.address = address;
        this.playerId = playerId;
        this.lastActivityTime = System.currentTimeMillis();
        this.pendingMessages = new ConcurrentHashMap<>();
    }

    public void updateActivity() {
        long oldTime = this.lastActivityTime;
        this.lastActivityTime = System.currentTimeMillis();
        Logger.debug("Peer {} activity updated: {} -> {}", address, oldTime, lastActivityTime);
    }

    public boolean isTimedOut(int timeoutMs) {
        long now = System.currentTimeMillis();
        long elapsed = now - lastActivityTime;
        boolean timedOut = elapsed > timeoutMs;

        Logger.debug("Timeout check for {}: now={}, lastActivity={}, elapsed={}ms, limit={}ms, timedOut={}",
                address, now, lastActivityTime, elapsed, timeoutMs, timedOut);

        return timedOut;
    }

    public void addPendingMessage(long msgSeq, byte[] data) {
        pendingMessages.put(msgSeq, new PendingMessage(data));
    }

    public void removePendingMessage(long msgSeq) { // когда пришел ack
        pendingMessages.remove(msgSeq);
    }

    public ConcurrentHashMap<Long, PendingMessage> getPendingMessages() {
        return pendingMessages;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public int getPlayerId() {
        return playerId;
    }

    public static class PendingMessage {
        private final byte[] data;
        private final long sentTime;
        private int retryCount;

        public PendingMessage(byte[] data) {
            this.data = data;
            this.sentTime = System.currentTimeMillis();
            this.retryCount = 0;
        }

        public boolean shouldRetry(int timeoutMs, int maxRetries) {
            if (retryCount >= maxRetries) {
                return false;
            }
            long elapsed = System.currentTimeMillis() - sentTime - (retryCount * timeoutMs);
            return elapsed > timeoutMs;
        }

        public void incrementRetry() {
            retryCount++;
        }

        public byte[] getData() {
            return data;
        }

        public int getRetryCount() {
            return retryCount;
        }
    }
}
