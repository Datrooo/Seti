package org.example.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IdGenerator {
    private static final AtomicInteger playerIdCounter = new AtomicInteger(1);
    private static final AtomicLong messageSeqCounter = new AtomicLong(0);

    public static int generatePlayerId() {
        return playerIdCounter.getAndIncrement();
    }

    public static long generateMessageSeq() {
        return messageSeqCounter.getAndIncrement();
    }

    public static void setNextPlayerId(int nextId) {
        playerIdCounter.set(nextId);
    }

    public static void resetPlayerIds() {
        playerIdCounter.set(1);
    }

    public static void resetMessageSeq() {
        messageSeqCounter.set(0);
    }
}
