package org.example.game.engine;

import org.example.game.model.Coord;
import org.example.game.model.GameConfig;
import org.example.game.model.GameState;

import java.util.Random;

public class FoodSpawner {
    private final TorusField field;
    private final Random random;

    public FoodSpawner(TorusField field) {
        this.field = field;
        this.random = new Random();
    }

    public void spawnFood(GameState state) {
        GameConfig config = state.getConfig();
        int requiredFood = config.getTotalFood(state.getPlayerCount());
        int currentFood = state.getFoods().size();

        int foodToSpawn = requiredFood - currentFood;

        for (int i = 0; i < foodToSpawn; i++) {
            try {
                Coord emptyCell = field.getRandomEmptyCell(coord ->
                        state.isCellOccupied(coord) || state.isFoodAt(coord)
                );
                state.addFood(emptyCell);
            } catch (IllegalStateException e) {
                // Нет свободных клеток
                break;
            }
        }
    }

    public void spawnFoodFromDeadSnake(GameState state, java.util.List<Coord> snakeBody) {
        float deadFoodProb = state.getConfig().deadFoodProb();

        for (Coord coord : snakeBody) {
            if (random.nextFloat() < deadFoodProb) {
                state.addFood(coord);
            }
        }
    }
}
