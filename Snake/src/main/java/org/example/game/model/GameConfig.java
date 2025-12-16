package org.example.game.model;

import org.example.util.Config;

public record GameConfig(
        int width,
        int height,
        int foodStatic,
        int foodPerPlayer,
        int stateDelayMs,
        float deadFoodProb,
        int pingDelayMs,
        int nodeTimeoutMs
) {

    public static GameConfig createDefault() {
        return new GameConfig(
                Config.DEFAULT_WIDTH,
                Config.DEFAULT_HEIGHT,
                Config.DEFAULT_FOOD_STATIC,
                Config.DEFAULT_FOOD_PER_PLAYER,
                Config.DEFAULT_STATE_DELAY_MS,
                Config.DEFAULT_DEAD_FOOD_PROB,
                Config.PING_DELAY_MS,
                Config.NODE_TIMEOUT_MS
        );
    }

    public int getTotalFood(int playerCount) {
        return foodStatic + foodPerPlayer * playerCount;
    }
}
