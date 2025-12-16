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
        int stateDelay = Config.DEFAULT_STATE_DELAY_MS;
        int pingDelay = Math.max(1, stateDelay / 10);          // по тексту ТЗ: stateDelay/10
        int nodeTimeout = Math.max(1, (int) (0.8 * stateDelay)); // по тексту ТЗ: 0.8*stateDelay
        return new GameConfig(
                Config.DEFAULT_WIDTH,
                Config.DEFAULT_HEIGHT,
                Config.DEFAULT_FOOD_STATIC,
                Config.DEFAULT_FOOD_PER_PLAYER,
                stateDelay,
                Config.DEFAULT_DEAD_FOOD_PROB,
                pingDelay,
                nodeTimeout
        );
    }


    public int getTotalFood(int playerCount) {
        return foodStatic + foodPerPlayer * playerCount;
    }
}
