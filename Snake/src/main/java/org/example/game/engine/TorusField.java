package org.example.game.engine;

import org.example.game.model.Coord;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TorusField {
    private final int width;
    private final int height;
    private final Random random;

    public TorusField(int width, int height) {
        this.width = width;
        this.height = height;
        this.random = new Random();
    }

    public Coord getRandomEmptyCell(java.util.function.Predicate<Coord> isOccupied) {
        List<Coord> emptyCells = new ArrayList<>();

        for (int x = 0; x < width; x++) {
            for (int y = 0; y < height; y++) {
                Coord coord = new Coord(x, y);
                if (!isOccupied.test(coord)) {
                    emptyCells.add(coord);
                }
            }
        }

        if (emptyCells.isEmpty()) {
            throw new IllegalStateException("No empty cells available on field");
        }

        return emptyCells.get(random.nextInt(emptyCells.size()));
    }

    public int getWidth() {
        return width;
    }
    public int getHeight() {
        return height;
    }
}
