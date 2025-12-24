package org.example.game.engine;

import org.example.game.model.Coord;
import org.example.game.model.Direction;
import org.example.game.model.GameState;
import org.example.game.model.Snake;
import org.example.util.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SnakeController {
    private final TorusField field;
    private final Random random;

    public SnakeController(TorusField field) {
        this.field = field;
        this.random = new Random();
    }

    
    public Snake createSnake(int playerId, GameState state) throws IllegalStateException {
        SnakePlacement placement = findEmpty5x5Square(state);
        
        if (placement == null) {
            throw new IllegalStateException("No suitable 5x5 square found for new snake");
        }

        Snake snake = new Snake(playerId, placement.head, placement.direction);
        
        snake.move(field.getWidth(), field.getHeight(), true);
        
        Logger.info("Created snake for player {}: head={}, tail={}, direction={}", 
                playerId, placement.head, placement.tail, placement.direction);
        
        return snake;
    }

    
    private SnakePlacement findEmpty5x5Square(GameState state) {
        int w = field.getWidth();
        int h = field.getHeight();
        
        List<Coord> possibleCenters = new ArrayList<>();
        
        for (int x = 0; x < w; x++) {
            for (int y = 0; y < h; y++) {
                Coord center = new Coord(x, y);
                if (is5x5SquareFree(center, state)) {
                    possibleCenters.add(center);
                }
            }
        }
        
        if (possibleCenters.isEmpty()) {
            return null;
        }
        
        java.util.Collections.shuffle(possibleCenters, random);
        
        for (Coord center : possibleCenters) {
            SnakePlacement placement = tryPlaceSnakeAt(center, state);
            if (placement != null) {
                return placement;
            }
        }
        
        return null;
    }

    private boolean is5x5SquareFree(Coord center, GameState state) {
        int w = field.getWidth();
        int h = field.getHeight();
        
        // Проверяем все клетки в квадрате 5x5 вокруг центра
        for (int dx = -2; dx <= 2; dx++) {
            for (int dy = -2; dy <= 2; dy++) {
                int x = (center.x() + dx + w) % w;
                int y = (center.y() + dy + h) % h;
                Coord cell = new Coord(x, y);
                
                if (state.isCellOccupied(cell)) {
                    return false;
                }
            }
        }
        
        return true;
    }

    private SnakePlacement tryPlaceSnakeAt(Coord center, GameState state) {
        Direction[] directions = Direction.values();
        List<Direction> shuffled = new ArrayList<>(List.of(directions));
        java.util.Collections.shuffle(shuffled, random);
        
        int w = field.getWidth();
        int h = field.getHeight();
        
        for (Direction dir : shuffled) {
            Coord tail = center.move(dir.opposite()).wrap(w, h);
            
            if (!state.isFoodAt(center) && !state.isFoodAt(tail)) {
                return new SnakePlacement(center, tail, dir);
            }
        }
        
        return null;
    }
    
    private record SnakePlacement(Coord head, Coord tail, Direction direction) {}
}
