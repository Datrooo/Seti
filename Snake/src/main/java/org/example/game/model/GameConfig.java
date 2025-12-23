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
    public int getTotalFood(int playerCount) {
        return foodStatic + foodPerPlayer * playerCount;
    }
}
