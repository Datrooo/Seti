package org.example.view;

import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.paint.Color;
import org.example.game.model.Coord;
import org.example.game.model.GameState;
import org.example.game.model.Snake;

import java.util.HashMap;
import java.util.Map;

public class GameRenderer {
    private final Canvas canvas;
    private final GraphicsContext gc;
    private final Map<Integer, Color> playerColors;
    private int cellSize;

    private static final Color BACKGROUND_COLOR = Color.rgb(44, 62, 80);
    private static final Color GRID_COLOR = Color.rgb(52, 73, 94);
    private static final Color FOOD_COLOR = Color.rgb(231, 76, 60);

    public GameRenderer(Canvas canvas) {
        this.canvas = canvas;
        this.gc = canvas.getGraphicsContext2D();
        this.playerColors = new HashMap<>();
        this.cellSize = 20;
    }

    public void render(GameState state) {
        if (state == null) {
            return;
        }

        // Вычисляем размер клетки
        int fieldWidth = state.getConfig().width();
        int fieldHeight = state.getConfig().height();

        double canvasWidth = canvas.getWidth();
        double canvasHeight = canvas.getHeight();

        cellSize = (int) Math.min(canvasWidth / fieldWidth, canvasHeight / fieldHeight);

        // Очищаем canvas
        gc.setFill(BACKGROUND_COLOR);
        gc.fillRect(0, 0, canvasWidth, canvasHeight);

        // Рисуем сетку
        drawGrid(fieldWidth, fieldHeight);

        // Рисуем еду
        drawFood(state);

        // Рисуем змеек
        drawSnakes(state);
    }

    private void drawGrid(int width, int height) {
        gc.setStroke(GRID_COLOR);
        gc.setLineWidth(1);

        // Вертикальные линии
        for (int x = 0; x <= width; x++) {
            gc.strokeLine(x * cellSize, 0, x * cellSize, height * cellSize);
        }

        // Горизонтальные линии
        for (int y = 0; y <= height; y++) {
            gc.strokeLine(0, y * cellSize, width * cellSize, y * cellSize);
        }
    }

    private void drawFood(GameState state) {
        gc.setFill(FOOD_COLOR);

        for (Coord food : state.getFoods()) {
            drawCell(food.x(), food.y());
        }
    }

    private void drawSnakes(GameState state) {
        for (Snake snake : state.getSnakes()) {
            Color color = getPlayerColor(snake.getPlayerId());

            if (snake.isZombie()) {
                // Зомби - полупрозрачные
                color = Color.color(color.getRed(), color.getGreen(), color.getBlue(), 0.5);
            }

            gc.setFill(color);

            // Рисуем тело змейки
            for (int i = 0; i < snake.getBody().size(); i++) {
                Coord segment = snake.getBody().get(i);
                drawCell(segment.x(), segment.y());

                // Голова - ярче
                if (i == 0) {
                    gc.setFill(color.brighter());
                    drawCell(segment.x(), segment.y());
                    gc.setFill(color);
                }
            }
        }
    }

    private void drawCell(int x, int y) {
        gc.fillRect(
                x * cellSize + 1,
                y * cellSize + 1,
                cellSize - 2,
                cellSize - 2
        );
    }

    private Color getPlayerColor(int playerId) {
        return playerColors.computeIfAbsent(playerId, id -> {
            // Генерируем цвет на основе ID
            double hue = (id * 137.508) % 360; // Золотое сечение для распределения цветов
            return Color.hsb(hue, 0.7, 0.9);
        });
    }
}
