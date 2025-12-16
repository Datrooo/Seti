package org.example.game.engine;

import org.example.game.model.Coord;
import org.example.game.model.Direction;
import org.example.game.model.GameState;
import org.example.game.model.Snake;

import java.util.Random;

public class SnakeController {
    private final TorusField field;
    private final Random random;

    public SnakeController(TorusField field) {
        this.field = field;
        this.random = new Random();
    }

    /**
     * Создает новую змейку на свободном месте
     */
    public Snake createSnake(int playerId, GameState state) {
        // Пытаемся найти место для змейки размером 2 клетки
        Coord headPosition = findStartPosition(state);
        Direction initialDirection = getRandomDirection();

        Snake snake = new Snake(playerId, headPosition, initialDirection);

        // Добавляем второй сегмент (хвост)
        Coord tailPosition = headPosition
                .move(initialDirection.opposite())
                .wrap(field.getWidth(), field.getHeight());

        // Проверяем, что хвост не занят
        if (state.isCellOccupied(tailPosition)) {
            // Пробуем другое направление
            for (Direction dir : Direction.values()) {
                tailPosition = headPosition
                        .move(dir.opposite())
                        .wrap(field.getWidth(), field.getHeight());

                if (!state.isCellOccupied(tailPosition)) {
                    snake = new Snake(playerId, headPosition, dir);
                    break;
                }
            }
        }

        // Растим змейку на 1 сегмент (чтобы получить длину 2)
        snake.move(field.getWidth(), field.getHeight(), true);

        return snake;
    }

    /**
     * Перемещает все змейки
     */
    public void moveAllSnakes(GameState state) {
        for (Snake snake : state.getSnakes()) {
            snake.move(field.getWidth(), field.getHeight(), false);
        }
    }

    /**
     * Перемещает змейку с ростом (после поедания еды)
     */
    public void moveSnakeWithGrowth(Snake snake) {
        snake.move(field.getWidth(), field.getHeight(), true);
    }

    /**
     * Автоматическое управление змейкой-зомби
     */
    public void controlZombie(Snake zombie) {
        if (!zombie.isZombie()) {
            return;
        }

        // Зомби продолжает двигаться в текущем направлении
        // Можно добавить простую логику избегания столкновений
        Direction current = zombie.getHeadDirection();
        Coord nextPos = zombie.getHead()
                .move(current)
                .wrap(field.getWidth(), field.getHeight());

        // Продолжаем движение в текущем направлении
        zombie.move(field.getWidth(), field.getHeight(), false);
    }

    private Coord findStartPosition(GameState state) {
        return field.getRandomEmptyCell(coord ->
                state.isCellOccupied(coord) || state.isFoodAt(coord)
        );
    }

    private Direction getRandomDirection() {
        Direction[] directions = Direction.values();
        return directions[random.nextInt(directions.length)];
    }
}
