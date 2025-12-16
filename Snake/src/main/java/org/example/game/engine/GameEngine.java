package org.example.game.engine;

import org.example.game.model.*;
import org.example.util.Logger;

import java.util.*;

public class GameEngine {
    private GameState gameState;

    private final TorusField field;
    private final CollisionDetector collisionDetector;
    private final FoodSpawner foodSpawner;
    private final SnakeController snakeController;

    public GameEngine(GameConfig config) {
        this.gameState = new GameState(config);
        this.field = new TorusField(config.width(), config.height());
        this.collisionDetector = new CollisionDetector();
        this.foodSpawner = new FoodSpawner(field);
        this.snakeController = new SnakeController(field);

        // начальная еда
        foodSpawner.spawnFood(gameState);
    }

    /**
     * Главный тик.
     * Порядок ближе к ТЗ:
     * 1) вычисляем, кто съест еду (по следующей клетке головы)
     * 2) списываем еду/даём очки
     * 3) двигаем змей (grow = ateFood)
     * 4) детектим коллизии и удаляем умерших
     * 5) досыпаем еду
     */
    public synchronized void update() {
        int w = field.getWidth();
        int h = field.getHeight();

        // 1) предсказание следующей головы и факта "съел еду"
        Map<Integer, Boolean> ateFood = new HashMap<>();
        Set<Coord> eatenFoodCells = new HashSet<>();

        for (Snake snake : gameState.getSnakes()) {
            // и ALIVE, и ZOMBIE двигаются по правилам (ZOMBIE просто не управляется)
            if (!(snake.isAlive() || snake.isZombie())) continue;

            Direction dir = snake.getDirectionForNextMove(); // добавим в Snake.java (ниже)
            Coord nextHead = snake.getHead().move(dir).wrap(w, h);

            boolean eat = gameState.isFoodAt(nextHead);
            ateFood.put(snake.getPlayerId(), eat);

            if (eat) {
                eatenFoodCells.add(nextHead);
            }
        }

        // 2) очки всем, кто съел (даже если несколько голов на одной еде)
        for (Snake snake : gameState.getSnakes()) {
            if (!snake.isAlive()) continue; // очки только живым игрокам
            if (Boolean.TRUE.equals(ateFood.get(snake.getPlayerId()))) {
                gameState.getPlayer(snake.getPlayerId()).ifPresent(Player::incrementScore);
            }
        }
        // удалить съеденную еду (каждая клетка ровно 1 раз)
        eatenFoodCells.forEach(gameState::removeFood);

        // 3) движение (grow зависит от того, была ли еда в целевой клетке)
        for (Snake snake : gameState.getSnakes()) {
            if (!(snake.isAlive() || snake.isZombie())) continue;
            boolean grow = Boolean.TRUE.equals(ateFood.get(snake.getPlayerId()));
            snake.move(w, h, grow);
        }

        // 4) коллизии (убиваем и ALIVE, и ZOMBIE)
        Set<Integer> deadSnakeIds = collisionDetector.detectCollisions(gameState.getSnakes());

        for (int deadId : deadSnakeIds) {
            gameState.getSnake(deadId).ifPresent(snake -> {
                // тело в еду
                foodSpawner.spawnFoodFromDeadSnake(gameState, snake.getBody());
                // убрать змейку
                gameState.removeSnake(deadId);
            });

            // убрать игрока, если он был (у ZOMBIE игрок уже мог быть удалён при выходе)
            if (gameState.getPlayer(deadId).isPresent()) {
                gameState.removePlayer(deadId);
            }
        }

        // 5) досыпаем еду до нормы
        foodSpawner.spawnFood(gameState);

        // 6) номер состояния
        gameState.incrementStateOrder();

        Logger.debug(
                "Game state updated: order={}, players={}, snakes={}, food={}",
                gameState.getStateOrder(),
                gameState.getPlayerCount(),
                gameState.getSnakes().size(),
                gameState.getFoods().size()
        );
    }

    public synchronized Snake addPlayer(Player player) {
        gameState.addPlayer(player);

        // ВАЖНО: не создаём Snake вручную, используем ваш же SnakeController
        Snake snake = snakeController.createSnake(player.getId(), gameState);
        gameState.addSnake(snake);

        Logger.info("Player {} joined the game with snake at {}", player.getName(), snake.getHead());
        return snake;
    }

    public synchronized void removePlayer(int playerId) {
        // по ТЗ при выходе игрока змейка становится ZOMBIE
        gameState.getSnake(playerId).ifPresent(snake -> {
            if (snake.isAlive()) {
                snake.kill(); // делает ZOMBIE
                Logger.info("Player {} left, snake became zombie", playerId);
            } else if (snake.isZombie()) {
                // если уже ZOMBIE и игрок ушёл давно — можно удалить змейку
                // (оставляю как было у вас: удаляем окончательно)
                //gameState.removeSnake(playerId);
            }
        });

        gameState.removePlayer(playerId);
    }

    public synchronized void handleSteer(int playerId, Direction direction) {
        Optional<Snake> snakeOpt = gameState.getSnake(playerId);
        if (snakeOpt.isEmpty()) {
            Logger.warn("Steer command for non-existent snake: {}", playerId);
            return;
        }

        Snake snake = snakeOpt.get();
        if (!snake.isAlive()) {
            Logger.debug("Steer command ignored for dead/zombie snake: {}", playerId);
            return;
        }

        snake.setDirection(direction); // кладёт в pendingDirection
        Logger.debug("Player {} changed direction to {}", playerId, direction);
    }

    public synchronized GameState getGameState() {
        return gameState.copy();
    }

    public synchronized void setGameState(GameState newState) {
        Logger.debug("setGameState: restoring state with {} players, {} snakes",
                newState.getPlayerCount(), newState.getSnakes().size());

        this.gameState = newState.copy();

        Logger.debug("State restored: players={}, snakes={}",
                gameState.getPlayerCount(), gameState.getSnakes().size());
    }
}
