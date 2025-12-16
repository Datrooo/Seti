package org.example.service;

import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyEvent;
import org.example.game.model.Direction;
import org.example.util.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Сервис для обработки пользовательского ввода
 */
public class InputService {
    private final GameService gameService;
    private final Map<KeyCode, Direction> keyBindings;
    private Direction lastDirection;
    private long lastSteerTime;
    private static final long MIN_STEER_INTERVAL_MS = 50; // Минимальный интервал между командами

    public InputService(GameService gameService) {
        this.gameService = gameService;
        this.keyBindings = new HashMap<>();
        this.lastDirection = null;
        this.lastSteerTime = 0;

        // Настройка привязок клавиш по умолчанию
        setupDefaultBindings();
    }

    /**
     * Настраивает привязки клавиш по умолчанию
     */
    private void setupDefaultBindings() {
        // Стрелки
        keyBindings.put(KeyCode.UP, Direction.UP);
        keyBindings.put(KeyCode.DOWN, Direction.DOWN);
        keyBindings.put(KeyCode.LEFT, Direction.LEFT);
        keyBindings.put(KeyCode.RIGHT, Direction.RIGHT);

        // WASD
        keyBindings.put(KeyCode.W, Direction.UP);
        keyBindings.put(KeyCode.S, Direction.DOWN);
        keyBindings.put(KeyCode.A, Direction.LEFT);
        keyBindings.put(KeyCode.D, Direction.RIGHT);
    }

    /**
     * Обрабатывает нажатие клавиши
     */
    public void handleKeyPressed(KeyEvent event) {
        KeyCode key = event.getCode();
        Direction direction = keyBindings.get(key);

        if (direction == null) {
            return;
        }

        long currentTime = System.currentTimeMillis();

        // Проверяем, не слишком ли часто отправляем команды
        if (currentTime - lastSteerTime < MIN_STEER_INTERVAL_MS) {
            return;
        }

        // Не отправляем одно и то же направление подряд
        if (direction == lastDirection) {
            return;
        }

        gameService.steer(direction);
        lastDirection = direction;
        lastSteerTime = currentTime;

        Logger.debug("Steer command: {} -> {}", key, direction);
    }

    /**
     * Сбрасывает последнее направление (для новой игры)
     */
    public void reset() {
        lastDirection = null;
        lastSteerTime = 0;
    }

    /**
     * Устанавливает привязку клавиши к направлению
     */
    public void setKeyBinding(KeyCode key, Direction direction) {
        keyBindings.put(key, direction);
    }

    /**
     * Удаляет привязку клавиши
     */
    public void removeKeyBinding(KeyCode key) {
        keyBindings.remove(key);
    }

    /**
     * Очищает все привязки
     */
    public void clearBindings() {
        keyBindings.clear();
    }

    public Map<KeyCode, Direction> getKeyBindings() {
        return new HashMap<>(keyBindings);
    }
}
