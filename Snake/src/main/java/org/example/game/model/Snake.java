package org.example.game.model;

import java.util.ArrayList;
import java.util.List;

public class Snake {
    private final int playerId;
    private final List<Coord> body;
    private Direction headDirection;
    private SnakeState state;
    private Direction pendingDirection;

    public Snake(int playerId, Coord headPosition, Direction initialDirection) {
        this.playerId = playerId;
        this.body = new ArrayList<>();
        this.body.add(headPosition);
        this.headDirection = initialDirection;
        this.state = SnakeState.ALIVE;
        this.pendingDirection = null;
    }

    public Snake(Snake other) {
        this.playerId = other.playerId;
        this.body = new ArrayList<>(other.body);
        this.headDirection = other.headDirection;
        this.state = other.state;
        this.pendingDirection = other.pendingDirection;
    }

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
        if (!newDirection.isOpposite(headDirection)) {
            this.pendingDirection = newDirection;
        }
    }

    public void move(int fieldWidth, int fieldHeight, boolean grow) {
        if (pendingDirection != null) {
            headDirection = pendingDirection;
            pendingDirection = null;
        }

        Coord newHead = getHead().move(headDirection).wrap(fieldWidth, fieldHeight);
        body.addFirst(newHead);

        if (!grow) {
            body.removeLast();
        }
    }

    public Snake copy() {
        Snake copied = new Snake(this.playerId, this.getHead(), this.headDirection);
        copied.body.clear();
        copied.body.addAll(this.body);
        copied.state = this.state;
        copied.pendingDirection = this.pendingDirection;
        return copied;
    }

    public void kill() {
        this.state = SnakeState.ZOMBIE;
    }

    public boolean isAlive() {
        return state == SnakeState.ALIVE;
    }

    public void setAlive(boolean alive) {
        this.state = alive ? SnakeState.ALIVE : SnakeState.ZOMBIE;
    }

    public Direction getDirectionForNextMove() {
        return (pendingDirection != null) ? pendingDirection : headDirection;
    }


    public void setHeadDirection(Direction direction) {
        this.headDirection = direction;
    }

    public List<Coord> getBodyInternal() {
        return body;
    }

    public boolean isZombie() {
        return state == SnakeState.ZOMBIE;
    }

    public void setState(SnakeState state) {
        this.state = state;
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
