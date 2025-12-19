package org.example.controller;

import javafx.animation.AnimationTimer;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.canvas.Canvas;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import org.example.game.model.GameState;
import org.example.game.model.Player;
import org.example.game.model.Snake;
import org.example.service.GameService;
import org.example.service.InputService;
import org.example.util.Logger;
import org.example.view.GameRenderer;

public class GameController {

    @FXML private BorderPane root;
    @FXML private Canvas gameCanvas;
    @FXML private TableView<PlayerRow> playersTable;
    @FXML private TableColumn<PlayerRow, String> playerNameColumn;
    @FXML private TableColumn<PlayerRow, Integer> scoreColumn;
    @FXML private TableColumn<PlayerRow, String> roleColumn;
    @FXML private Label gameInfoLabel;
    @FXML private Label statusLabel;

    private GameService gameService;
    private InputService inputService;
    private GameRenderer renderer;
    private AnimationTimer gameLoop;

    @FXML
    private void initialize() {
        // Настраиваем таблицу игроков
        playerNameColumn.setCellValueFactory(new PropertyValueFactory<>("name"));
        scoreColumn.setCellValueFactory(new PropertyValueFactory<>("score"));
        roleColumn.setCellValueFactory(new PropertyValueFactory<>("role"));

        // Создаем renderer
        renderer = new GameRenderer(gameCanvas);

        Logger.info("GameController initialized");
    }

    public void setGameService(GameService gameService) {
        this.gameService = gameService;
        this.inputService = new InputService(gameService);

        // Регистрируем обработчик клавиатуры
        root.setOnKeyPressed(this::handleKeyPress);
        root.requestFocus();

        // Запускаем игровой цикл отрисовки
        startGameLoop();

        Logger.info("GameService connected to GameController");
    }

    private void handleKeyPress(KeyEvent event) {
        if (inputService != null) {
            inputService.handleKeyPressed(event);
        }
    }

    private void startGameLoop() {
        gameLoop = new AnimationTimer() {
            @Override
            public void handle(long now) {
                updateUI();
            }
        };
        gameLoop.start();
    }

    private int lastOrder = -1;

    private void updateUI() {
        if (gameService == null || !gameService.isActive()) return;
        GameState state = gameService.getGameState();
        if (state == null) return;

        int order = state.getStateOrder();
        if (order == lastOrder) return;
        lastOrder = order;

        renderer.render(state);
        updateGameInfo(state);
        updatePlayersTable(state);
    }




    private void updateGameInfo(GameState state) {
        Player localPlayer = gameService.getLocalPlayer();
        if (localPlayer != null) {
            gameInfoLabel.setText(String.format(
                    "Game: %s | Role: %s | State: #%d",
                    gameService.getContext() != null ? gameService.getContext().getGameName() : "Unknown",
                    gameService.getCurrentRole(),
                    state.getStateOrder()
            ));

            statusLabel.setText(String.format(
                    "Players: %d | Food: %d",
                    state.getPlayerCount(),
                    state.getFoods().size()
            ));
        }
    }

    private void updatePlayersTable(GameState state) {
        playersTable.getItems().clear();

        for (Snake snake : state.getSnakes()) {
            String playerName = "Unknown";

            for (Player p : state.getPlayers()) {
                if (p.getId() == snake.getPlayerId()) {
                    playerName = p.getName();
                    break;
                }
            }

            String status = switch (snake.getState()) {
                case ALIVE -> "🟢 Alive";
                case ZOMBIE -> "💀 Zombie";
                default -> "?";
            };

            playersTable.getItems().add(new PlayerRow(
                    playerName,
                    snake.getLength() * 10,  // score = длина * 10 (или возьми real score)
                    status
            ));
        }
    }


    @FXML private Button exitButton;


    @FXML
    private void onExitToMenu() {
        // Показываем диалог подтверждения
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Exit to Menu");
        alert.setHeaderText("Are you sure you want to leave the game?");
        alert.setContentText("Your progress will be lost.");

        alert.showAndWait().ifPresent(response -> {
            if (response == ButtonType.OK) {
                exitToMainMenu();
            }
        });
    }

    private void exitToMainMenu() {
        try {
            // Останавливаем игровой цикл
            if (gameLoop != null) {
                gameLoop.stop();
            }

            // Выходим из игры
            if (gameService != null) {
                gameService.leaveGame();
            }

            Logger.info("Exiting to main menu");

            // Загружаем главное меню
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/main-menu.fxml"));
            Parent root = loader.load();

            // Получаем текущую Stage
            Stage stage = (Stage) exitButton.getScene().getWindow();

            Scene scene = new Scene(root, 800, 600);
            scene.getStylesheets().add(getClass().getResource("/css/styles.css").toExternalForm());

            stage.setScene(scene);
            stage.setTitle("Snake Game - Multiplayer");

        } catch (Exception e) {
            Logger.error("Failed to return to main menu: {}", e.getMessage(), e);
            showError("Failed to return to main menu");
        }
    }

    private void showError(String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Error");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }


    public void stop() {
        if (gameLoop != null) {
            gameLoop.stop();
        }
    }

    // Класс для отображения игрока в таблице
    public static class PlayerRow {
        private final String name;
        private final int score;
        private final String role;

        public PlayerRow(String name, int score, String role) {
            this.name = name;
            this.score = score;
            this.role = role;
        }

        public String getName() { return name; }
        public int getScore() { return score; }
        public String getRole() { return role; }
    }
}
