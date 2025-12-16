package org.example.game.engine;

import org.example.game.model.Coord;
import org.example.game.model.Snake;

import java.util.*;

public class CollisionDetector {

    public Set<Integer> detectCollisions(Collection<Snake> snakes) {
        Set<Integer> dead = new HashSet<>();

        // 1) занятые клетки телами (без голов) — для правила "можно идти по хвосту"
        Set<Coord> occupiedBodies = new HashSet<>();
        for (Snake snake : snakes) {
            if (!(snake.isAlive() || snake.isZombie())) continue;

            List<Coord> body = snake.getBody();
            for (int i = 1; i < body.size(); i++) {
                occupiedBodies.add(body.get(i));
            }
        }

        // 2) головы в одну клетку (все умирают)
        Map<Coord, List<Integer>> headsAt = new HashMap<>();
        for (Snake snake : snakes) {
            if (!(snake.isAlive() || snake.isZombie())) continue;

            headsAt.computeIfAbsent(snake.getHead(), k -> new ArrayList<>()).add(snake.getPlayerId());
        }
        for (Map.Entry<Coord, List<Integer>> e : headsAt.entrySet()) {
            if (e.getValue().size() > 1) {
                dead.addAll(e.getValue());
            }
        }

        // 3) голова в тело (своё или чужое)
        for (Snake snake : snakes) {
            if (!(snake.isAlive() || snake.isZombie())) continue;

            if (occupiedBodies.contains(snake.getHead())) {
                dead.add(snake.getPlayerId());
            }
        }

        return dead;
    }
}
