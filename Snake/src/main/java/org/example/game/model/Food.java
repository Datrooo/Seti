package org.example.game.model;

public record Food(Coord position) {

    @Override
    public String toString() {
        return String.format("Food at (%d, %d)", position.x(), position.y());
    }
}