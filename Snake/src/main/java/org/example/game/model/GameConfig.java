package org.example.game.model;

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
