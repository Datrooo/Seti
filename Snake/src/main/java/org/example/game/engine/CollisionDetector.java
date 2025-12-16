package org.example.game.engine;

import  org.example.game.model.Coord;
import  org.example.game.model.Snake;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CollisionDetector {

    /**
     * Проверяет столкновения змейки с другими змейками
     * @return Set ID змеек, которые умерли в результате столкновения
     */
    public Set<Integer> detectCollisions(Collection<Snake> snakes) {
        Set<Integer> deadSnakes = new HashSet<>();

        // Строим карту занятых клеток (без голов)
        Set<Coord> occupiedCells = new HashSet<>();
        for (Snake snake : snakes) {
            List<Coord> body = snake.getBody();
            // Добавляем все клетки кроме головы
            for (int i = 1; i < body.size(); i++) {
                occupiedCells.add(body.get(i));
            }
        }

        // Проверяем голову каждой живой змейки
        for (Snake snake : snakes) {
            if (!snake.isAlive()) {
                continue;
            }

            Coord head = snake.getHead();

            // Столкновение с телом (своим или чужим)
            if (occupiedCells.contains(head)) {
                deadSnakes.add(snake.getPlayerId());
                continue;
            }

            // Столкновение головы с головой другой змейки
            for (Snake other : snakes) {
                if (other.getPlayerId() == snake.getPlayerId() || !other.isAlive()) {
                    continue;
                }

                if (head.equals(other.getHead())) {
                    // Обе змейки умирают при лобовом столкновении
                    deadSnakes.add(snake.getPlayerId());
                    deadSnakes.add(other.getPlayerId());
                }
            }
        }

        return deadSnakes;
    }

    /**
     * Проверяет, съела ли змейка еду
     */
    public boolean checkFoodCollision(Snake snake, Coord foodPosition) {
        return snake.getHead().equals(foodPosition);
    }
}
