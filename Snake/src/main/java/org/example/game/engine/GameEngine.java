package org.example.game.engine;

import org.example.game.model.*;
import org.example.util.Logger;

import java.util.*;

public class GameEngine {
    private GameState gameState;

    private final TorusField field;
    private final FoodSpawner foodSpawner;
    private final SnakeController snakeController;

    public GameEngine(GameConfig config) {
        this.gameState = new GameState(config);
        this.field = new TorusField(config.width(), config.height());
        this.foodSpawner = new FoodSpawner(field);
        this.snakeController = new SnakeController(field);
        foodSpawner.spawnFood(gameState);
    }


    public synchronized void update() {
        int w = field.getWidth();
        int h = field.getHeight();

        Map<Integer, Boolean> ateFood = new HashMap<>();
        Set<Coord> eatenFoodCells = new HashSet<>();

        for (Snake snake : gameState.getSnakes()) {
            if (!(snake.isAlive() || snake.isZombie())) continue;

            Direction dir = snake.getDirectionForNextMove();
            Coord nextHead = snake.getHead().move(dir).wrap(w, h);

            boolean eat = gameState.isFoodAt(nextHead);
            ateFood.put(snake.getPlayerId(), eat);

            if (eat) {
                eatenFoodCells.add(nextHead);
            }
        }

        for (Snake snake : gameState.getSnakes()) {
            if (!snake.isAlive()) continue;
            if (Boolean.TRUE.equals(ateFood.get(snake.getPlayerId()))) {
                gameState.getPlayer(snake.getPlayerId()).ifPresent(Player::incrementScore);
            }
        }
        eatenFoodCells.forEach(gameState::removeFood);

        for (Snake snake : gameState.getSnakes()) {
            if (!(snake.isAlive() || snake.isZombie())) continue;
            boolean grow = Boolean.TRUE.equals(ateFood.get(snake.getPlayerId()));
            snake.move(w, h, grow);
        }

        Map<Coord, Integer> bodyOwner = new HashMap<>();
        for (Snake s : gameState.getSnakes()) {
            if (!(s.isAlive() || s.isZombie())) continue;

            List<Coord> body = s.getBody();
            for (int i = 1; i < body.size(); i++) {
                bodyOwner.put(body.get(i), s.getPlayerId());
            }
        }

        Map<Coord, List<Integer>> headsAt = new HashMap<>();
        for (Snake s : gameState.getSnakes()) {
            if (!(s.isAlive() || s.isZombie())) continue;
            headsAt.computeIfAbsent(s.getHead(), k -> new ArrayList<>()).add(s.getPlayerId());
        }

        Set<Integer> headOnDeaths = new HashSet<>();
        for (var e : headsAt.entrySet()) {
            if (e.getValue().size() > 1) headOnDeaths.addAll(e.getValue());
        }

        Set<Integer> deadSnakeIds = new HashSet<>();
        
        deadSnakeIds.addAll(headOnDeaths);
        
        for (Snake snake : gameState.getSnakes()) {
            if (!(snake.isAlive() || snake.isZombie())) continue;
            
            if (bodyOwner.containsKey(snake.getHead())) {
                deadSnakeIds.add(snake.getPlayerId());
            }
        }

        Map<Integer, Integer> killPoints = new HashMap<>();

        for (Snake victim : gameState.getSnakes()) {
            if (!(victim.isAlive() || victim.isZombie())) continue;

            int victimId = victim.getPlayerId();
            if (!deadSnakeIds.contains(victimId)) continue;
            if (headOnDeaths.contains(victimId)) continue;

            Integer killerId = bodyOwner.get(victim.getHead());
            if (killerId == null) continue;

            if (killerId == victimId) continue;
            if (deadSnakeIds.contains(killerId)) continue;

            killPoints.merge(killerId, 1, Integer::sum);
        }

        for (var e : killPoints.entrySet()) {
            int killerId = e.getKey();
            int cnt = e.getValue();
            gameState.getPlayer(killerId).ifPresent(p -> {
                for (int i = 0; i < cnt; i++) p.incrementScore();
            });
        }

        for (int deadId : deadSnakeIds) {
            gameState.getSnake(deadId).ifPresent(snake -> {
                foodSpawner.spawnFoodFromDeadSnake(gameState, snake.getBody());
                gameState.removeSnake(deadId);
            });

            if (gameState.getPlayer(deadId).isPresent()) {
                gameState.removePlayer(deadId);
            }
        }

        foodSpawner.spawnFood(gameState);

        gameState.incrementStateOrder();

        Logger.debug(
                "Game state updated: order={}, players={}, snakes={}, food={}",
                gameState.getStateOrder(),
                gameState.getPlayerCount(),
                gameState.getSnakes().size(),
                gameState.getFoods().size()
        );
    }

    public synchronized Snake addPlayer(Player player) throws IllegalStateException {
        gameState.addPlayer(player);

        Snake snake = snakeController.createSnake(player.getId(), gameState);
        gameState.addSnake(snake);

        Logger.info("Player {} joined the game with snake at {}", player.getName(), snake.getHead());
        return snake;
    }

    public synchronized void removePlayer(int playerId) {
        gameState.getSnake(playerId).ifPresent(snake -> {
            if (snake.isAlive()) {
                snake.kill();
                Logger.info("Player {} left, snake became zombie", playerId);
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

        snake.setDirection(direction);
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
