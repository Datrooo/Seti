package org.example.service;

import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyEvent;
import org.example.game.model.Direction;
import org.example.util.Logger;

import java.util.HashMap;
import java.util.Map;

public class InputService {
    private final GameService gameService;
    private final Map<KeyCode, Direction> keyBindings;
    private Direction lastDirection;
    private long lastSteerTime;
    private static final long MIN_STEER_INTERVAL_MS = 50;
    public InputService(GameService gameService) {
        this.gameService = gameService;
        this.keyBindings = new HashMap<>();
        this.lastDirection = null;
        this.lastSteerTime = 0;
        setupDefaultBindings();
    }

    private void setupDefaultBindings() {
        keyBindings.put(KeyCode.W, Direction.UP);
        keyBindings.put(KeyCode.S, Direction.DOWN);
        keyBindings.put(KeyCode.A, Direction.LEFT);
        keyBindings.put(KeyCode.D, Direction.RIGHT);
    }

    public void handleKeyPressed(KeyEvent event) {
        KeyCode key = event.getCode();
        Direction direction = keyBindings.get(key);

        if (direction == null) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime - lastSteerTime < MIN_STEER_INTERVAL_MS) {
            return;
        }

        if (direction == lastDirection) {
            return;
        }

        gameService.steer(direction);
        lastDirection = direction;
        lastSteerTime = currentTime;

        Logger.debug("Steer command: {} -> {}", key, direction);
    }
}
