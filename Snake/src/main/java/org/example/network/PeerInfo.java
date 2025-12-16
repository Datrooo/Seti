package org.example.network;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
        this.lastActivityTime = System.currentTimeMillis();
    }

    public boolean isTimedOut(int timeoutMs) {
        return System.currentTimeMillis() - lastActivityTime > timeoutMs;
    }

    public void addPendingMessage(long msgSeq, byte[] messageData) {
        pendingMessages.put(msgSeq, new PendingMessage(messageData));
    }

    public void removePendingMessage(long msgSeq) {
        pendingMessages.remove(msgSeq);
    }

    public PendingMessage getPendingMessage(long msgSeq) {
        return pendingMessages.get(msgSeq);
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

    public long getLastActivityTime() {
        return lastActivityTime;
    }

    public static class PendingMessage {
        private final byte[] data;
        private final long sendTime;
        private int retryCount;

        public PendingMessage(byte[] data) {
            this.data = data;
            this.sendTime = System.currentTimeMillis();
            this.retryCount = 0;
        }

        public byte[] getData() {
            return data;
        }

        public long getSendTime() {
            return sendTime;
        }

        public int getRetryCount() {
            return retryCount;
        }

        public void incrementRetry() {
            retryCount++;
        }

        public boolean shouldRetry(int ackTimeoutMs, int maxRetries) {
            return retryCount < maxRetries &&
                    System.currentTimeMillis() - sendTime > (long) ackTimeoutMs * (retryCount + 1);
        }
    }
}
