package org.example.game.engine;

import org.example.game.model.*;
import org.example.node.NodeRole;
import org.example.util.Logger;

import java.util.*;

public class GameEngine {
    private final GameState gameState;
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

        // Инициализируем начальную еду
        foodSpawner.spawnFood(gameState);
    }

    /**
     * Главный метод обновления состояния игры (вызывается каждый тик)
     */
    public synchronized void update() {
        // 1. Перемещаем все змейки
        snakeController.moveAllSnakes(gameState);

        // 2. Обрабатываем змеек-зомби
        for (Snake snake : gameState.getSnakes()) {
            if (snake.isZombie()) {
                snakeController.controlZombie(snake);
            }
        }

        // 3. Проверяем поедание еды
        checkFoodCollisions();

        // 4. Проверяем столкновения
        handleCollisions();

        // 5. Спавним недостающую еду
        foodSpawner.spawnFood(gameState);

        // 6. Удаляем мертвых игроков (таймаут)
        removeTimedOutPlayers();

        // 7. Увеличиваем номер состояния
        gameState.incrementStateOrder();

        Logger.debug("Game state updated: order={}, players={}, snakes={}, food={}",
                gameState.getStateOrder(),
                gameState.getPlayerCount(),
                gameState.getSnakes().size(),
                gameState.getFoods().size());
    }

    /**
     * Добавляет нового игрока в игру
     */
    public synchronized Snake addPlayer(Player player) {
        gameState.addPlayer(player);

        // Создаем змейку для игрока
        Snake snake = snakeController.createSnake(player.getId(), gameState);
        gameState.addSnake(snake);

        Logger.info("Player {} joined the game with snake at {}",
                player.getName(), snake.getHead());

        return snake;
    }

    /**
     * Удаляет игрока из игры
     */
    public synchronized void removePlayer(int playerId) {
        Optional<Snake> snakeOpt = gameState.getSnake(playerId);

        if (snakeOpt.isPresent()) {
            Snake snake = snakeOpt.get();

            // Если змейка жива, делаем её зомби
            if (snake.isAlive()) {
                snake.kill();
                Logger.info("Player {} left, snake became zombie", playerId);
            } else {
                // Если уже зомби, удаляем совсем
                gameState.removeSnake(playerId);
            }
        }

        gameState.removePlayer(playerId);
    }

    /**
     * Обрабатывает команду управления змейкой
     */
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

        snake.setDirection(direction);
        Logger.debug("Player {} changed direction to {}", playerId, direction);
    }

    /**
     * Получает копию текущего состояния игры (для отправки по сети)
     */
    public synchronized GameState getGameState() {
        return gameState.copy();
    }

    /**
     * Обновляет состояние игры (для клиентов, получающих StateMsg)
     */
    public synchronized void setGameState(GameState newState) {
        // Этот метод используется на клиентской стороне
        // Копируем данные из полученного состояния
        gameState.getPlayers().clear();
        newState.getPlayers().forEach(gameState::addPlayer);

        gameState.getSnakes().clear();
        newState.getSnakes().forEach(gameState::addSnake);

        gameState.getFoods().clear();
        newState.getFoods().forEach(gameState::addFood);
    }

    private void checkFoodCollisions() {
        Set<Coord> foodsToRemove = new HashSet<>();

        for (Snake snake : gameState.getSnakes()) {
            if (!snake.isAlive()) {
                continue;
            }

            Coord head = snake.getHead();

            // Проверяем, есть ли еда в клетке головы
            if (gameState.isFoodAt(head)) {
                foodsToRemove.add(head);

                // Увеличиваем счет игрока
                gameState.getPlayer(snake.getPlayerId())
                        .ifPresent(Player::incrementScore);

                // Растим змейку
                snakeController.moveSnakeWithGrowth(snake);

                Logger.info("Player {} ate food at {}, score: {}",
                        snake.getPlayerId(), head,
                        gameState.getPlayer(snake.getPlayerId())
                                .map(Player::getScore).orElse(0));
            }
        }

        // Удаляем съеденную еду
        foodsToRemove.forEach(gameState::removeFood);
    }

    private void handleCollisions() {
        Set<Integer> deadSnakeIds = collisionDetector.detectCollisions(
                gameState.getSnakes()
        );

        for (int deadId : deadSnakeIds) {
            Optional<Snake> snakeOpt = gameState.getSnake(deadId);

            if (snakeOpt.isEmpty()) {
                continue;
            }

            Snake snake = snakeOpt.get();

            if (snake.isAlive()) {
                List<Coord> body = snake.getBody();

                // Превращаем тело в еду
                foodSpawner.spawnFoodFromDeadSnake(gameState, body);

                // Убиваем змейку (делаем зомби)
                snake.kill();

                Logger.info("Player {} died in collision", deadId);
            }
        }
    }

    private void removeTimedOutPlayers() {
        int timeoutMs = gameState.getConfig().nodeTimeoutMs();
        List<Integer> timedOutPlayers = new ArrayList<>();

        for (Player player : gameState.getPlayers()) {
            if (player.isTimedOut(timeoutMs) && player.getRole() != NodeRole.MASTER) {
                timedOutPlayers.add(player.getId());
            }
        }

        for (int playerId : timedOutPlayers) {
            Logger.warn("Player {} timed out, removing", playerId);

            // Удаляем змейку полностью
            gameState.getSnake(playerId).ifPresent(snake -> {
                foodSpawner.spawnFoodFromDeadSnake(gameState, snake.getBody());
                gameState.removeSnake(playerId);
            });

            gameState.removePlayer(playerId);
        }
    }
}
