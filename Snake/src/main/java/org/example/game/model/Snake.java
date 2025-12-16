package org.example.game.model;

import java.util.ArrayList;
import java.util.List;

public class Snake {
    private final int playerId;
    private final List<Coord> body; // [0] - голова, [n-1] - хвост
    private Direction headDirection;
    private SnakeState state;
    private Direction pendingDirection; // Следующее направление (из SteerMsg)

    public Snake(int playerId, Coord headPosition, Direction initialDirection) {
        this.playerId = playerId;
        this.body = new ArrayList<>();
        this.body.add(headPosition);
        this.headDirection = initialDirection;
        this.state = SnakeState.ALIVE;
        this.pendingDirection = null;
    }

    // Конструктор для копирования
    public Snake(Snake other) {
        this.playerId = other.playerId;
        this.body = new ArrayList<>(other.body);
        this.headDirection = other.headDirection;
        this.state = other.state;
        this.pendingDirection = other.pendingDirection;
    }

    /**
     * Конструктор для восстановления змейки из списка координат (для десериализации)
     */
    public Snake(int playerId, List<Coord> bodyCoords, Direction headDirection, SnakeState state) {
        this.playerId = playerId;
        this.body = new ArrayList<>(bodyCoords);
        this.headDirection = headDirection;
        this.state = state;
        this.pendingDirection = null;
    }

    public Coord getHead() {
        return body.getFirst();
    }

    public Coord getTail() {
        return body.getLast();
    }

    public List<Coord> getBody() {
        return new ArrayList<>(body);
    }

    public int getLength() {
        return body.size();
    }

    public boolean contains(Coord coord) {
        return body.contains(coord);
    }

    public void setDirection(Direction newDirection) {
        // Нельзя повернуть на 180 градусов
        if (!newDirection.isOpposite(headDirection)) {
            this.pendingDirection = newDirection;
        }
    }

    public void move(int fieldWidth, int fieldHeight, boolean grow) {
        // Применяем отложенное направление
        if (pendingDirection != null) {
            headDirection = pendingDirection;
            pendingDirection = null;
        }

        // Вычисляем новую позицию головы
        Coord newHead = getHead().move(headDirection).wrap(fieldWidth, fieldHeight);

        // Добавляем новую голову
        body.addFirst(newHead);

        // Удаляем хвост, если не растем
        if (!grow) {
            body.removeLast();
        }
    }

    public void kill() {
        this.state = SnakeState.ZOMBIE;
    }

    public boolean isAlive() {
        return state == SnakeState.ALIVE;
    }

    public boolean isZombie() {
        return state == SnakeState.ZOMBIE;
    }

    public int getPlayerId() {
        return playerId;
    }

    public Direction getHeadDirection() {
        return headDirection;
    }

    public SnakeState getState() {
        return state;
    }

    public enum SnakeState {
        ALIVE,
        ZOMBIE
    }
}
