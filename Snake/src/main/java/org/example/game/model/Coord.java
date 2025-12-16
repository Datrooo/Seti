package org.example.game.model;

import java.util.Objects;

public record Coord(int x, int y) {

    public Coord move(Direction direction) {
        return switch (direction) {
            case UP -> new Coord(x, y - 1);
            case DOWN -> new Coord(x, y + 1);
            case LEFT -> new Coord(x - 1, y);
            case RIGHT -> new Coord(x + 1, y);
        };
    }

    public Coord wrap(int width, int height) {
        int newX = ((x % width) + width) % width;
        int newY = ((y % height) + height) % height;
        return new Coord(newX, newY);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Coord coord)) return false;
        return x == coord.x && y == coord.y;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }
}
