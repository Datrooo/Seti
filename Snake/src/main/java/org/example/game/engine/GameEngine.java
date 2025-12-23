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
        foodSpawner.spawnFood(gameState);
    }


    public synchronized void update() {
        int w = field.getWidth();
        int h = field.getHeight();

        // предсказание следующей головы и факта "съел еду"
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

        // очки всем, кто съел (даже если несколько голов на одной еде)
        for (Snake snake : gameState.getSnakes()) {
            if (!snake.isAlive()) continue; // очки только живым игрокам
            if (Boolean.TRUE.equals(ateFood.get(snake.getPlayerId()))) {
                gameState.getPlayer(snake.getPlayerId()).ifPresent(Player::incrementScore);
            }
        }
        eatenFoodCells.forEach(gameState::removeFood);

        // движение (grow зависит от того, была ли еда в целевой клетке)
        for (Snake snake : gameState.getSnakes()) {
            if (!(snake.isAlive() || snake.isZombie())) continue;
            boolean grow = Boolean.TRUE.equals(ateFood.get(snake.getPlayerId()));
            snake.move(w, h, grow);
        }

        // подготовка данных для начисления "киллов"
        // bodyOwner: каждая клетка тела (кроме головы) -> playerId владельца
        Map<Coord, Integer> bodyOwner = new HashMap<>();
        for (Snake s : gameState.getSnakes()) {
            if (!(s.isAlive() || s.isZombie())) continue;

            List<Coord> body = s.getBody();
            for (int i = 1; i < body.size(); i++) {
                bodyOwner.put(body.get(i), s.getPlayerId());
            }
        }

        // головы в одну клетку (head-on)
        Map<Coord, List<Integer>> headsAt = new HashMap<>();
        for (Snake s : gameState.getSnakes()) {
            if (!(s.isAlive() || s.isZombie())) continue;
            headsAt.computeIfAbsent(s.getHead(), k -> new ArrayList<>()).add(s.getPlayerId());
        }

        Set<Integer> headOnDeaths = new HashSet<>();
        for (var e : headsAt.entrySet()) {
            if (e.getValue().size() > 1) headOnDeaths.addAll(e.getValue());
        }

        // коллизии
        Set<Integer> deadSnakeIds = collisionDetector.detectCollisions(gameState.getSnakes());

        // начисление +1 за убийство:
        Map<Integer, Integer> killPoints = new HashMap<>();

        for (Snake victim : gameState.getSnakes()) {
            if (!(victim.isAlive() || victim.isZombie())) continue;

            int victimId = victim.getPlayerId();
            if (!deadSnakeIds.contains(victimId)) continue;      // начисляем только за реально умерших
            if (headOnDeaths.contains(victimId)) continue;       // head-on: по ТЗ очки за убийство не начисляем

            Integer killerId = bodyOwner.get(victim.getHead());  // голова victim попала в чьё-то тело?
            if (killerId == null) continue;

            if (killerId == victimId) continue;                  // сам в себя -> никому
            if (deadSnakeIds.contains(killerId)) continue;       // killer тоже умер на этом ходу -> не даём

            killPoints.merge(killerId, 1, Integer::sum);
        }

        for (var e : killPoints.entrySet()) {
            int killerId = e.getKey();
            int cnt = e.getValue();
            gameState.getPlayer(killerId).ifPresent(p -> {
                for (int i = 0; i < cnt; i++) p.incrementScore();
            });
        }

        // удаление мёртвых змей/игроков + превращение тела в еду
        for (int deadId : deadSnakeIds) {
            gameState.getSnake(deadId).ifPresent(snake -> {
                foodSpawner.spawnFoodFromDeadSnake(gameState, snake.getBody());
                gameState.removeSnake(deadId);
            });

            if (gameState.getPlayer(deadId).isPresent()) {
                gameState.removePlayer(deadId);
            }
        }

        //досыпаем еду до нормы
        foodSpawner.spawnFood(gameState);

        //номер состояния
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
                snake.kill(); // делает ZOMBIE
                Logger.info("Player {} left, snake became zombie", playerId);
            }
        });

        gameState.removePlayer(playerId);
    }

    /**
     * Обрабатывает команду поворота от игрока.
     * Важно: вызывающая сторона (MasterNode) должна проверять msg_seq,
     * чтобы более новые команды заменяли старые в пределах хода.
     * 
     * @param playerId ID игрока
     * @param direction новое направление движения
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

        snake.setDirection(direction); // в pendingDirection
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
