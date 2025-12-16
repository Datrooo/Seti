package org.example.util;

public class Config {
    // Network
    public static final String MULTICAST_ADDRESS = "239.192.0.4";
    public static final int MULTICAST_PORT = 9192;
    public static final int DEFAULT_PORT = 9190;

    // Timeouts
    public static final int PING_DELAY_MS = 1000;
    public static final int NODE_TIMEOUT_MS = 3000;
    public static final int ACK_TIMEOUT_MS = 500;
    public static final int MAX_RETRIES = 5;

    // Game defaults
    public static final int DEFAULT_WIDTH = 40;
    public static final int DEFAULT_HEIGHT = 30;
    public static final int DEFAULT_FOOD_STATIC = 5;
    public static final int DEFAULT_FOOD_PER_PLAYER = 1;
    public static final int DEFAULT_STATE_DELAY_MS = 100;
    public static final float DEFAULT_DEAD_FOOD_PROB = 0.1f;

    // UI
    public static final int CELL_SIZE = 20;
    public static final int WINDOW_WIDTH = 1000;
    public static final int WINDOW_HEIGHT = 700;

    private Config() {}
}
