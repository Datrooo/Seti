package org.example.controller;

import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.stage.Stage;
import org.example.node.NodeRole;
import org.example.protocol.SnakesProto;
import org.example.service.DiscoveryService;
import org.example.service.GameService;
import org.example.util.Logger;

import java.net.InetSocketAddress;

public class LobbyController {

    @FXML private TableView<GameInfo> gamesTable;
    @FXML private TableColumn<GameInfo, String> nameColumn;
    @FXML private TableColumn<GameInfo, String> sizeColumn;
    @FXML private TableColumn<GameInfo, Integer> playersColumn;
    @FXML private TableColumn<GameInfo, String> statusColumn;
    @FXML private TextField playerNameField;
    @FXML private RadioButton playRadio;
    @FXML private RadioButton spectateRadio;
    @FXML private Button joinButton;
    @FXML private Button backButton;

    private final ObservableList<GameInfo> gamesList = FXCollections.observableArrayList();
    private DiscoveryService discoveryService;
    private GameService gameService;

    @FXML
    private void initialize() {
        // Настраиваем колонки таблицы
        nameColumn.setCellValueFactory(new PropertyValueFactory<>("name"));
        sizeColumn.setCellValueFactory(new PropertyValueFactory<>("size"));
        playersColumn.setCellValueFactory(new PropertyValueFactory<>("players"));
        statusColumn.setCellValueFactory(new PropertyValueFactory<>("status"));

        gamesTable.setItems(gamesList);

        // Двойной клик для присоединения
        gamesTable.setOnMouseClicked(event -> {
            if (event.getClickCount() == 2) {
                onJoin();
            }
        });

        // Radio buttons
        ToggleGroup group = new ToggleGroup();
        playRadio.setToggleGroup(group);
        spectateRadio.setToggleGroup(group);
        playRadio.setSelected(true);

        // Запускаем поиск игр
        startDiscovery();
    }


    private void startDiscovery() {
        discoveryService = new DiscoveryService();

        try {
            discoveryService.start();

            // Подписываемся на появление новых игр
            // Подписываемся на появление новых игр
            discoveryService.addGameAddedListener(game -> {
                Platform.runLater(() -> {
                    GameInfo info = new GameInfo(
                            game.getGameName(),
                            game.getFieldWidth() + "x" + game.getFieldHeight(),
                            game.getPlayerCount(),
                            game.canJoin() ? "Open" : "Full",
                            game.getAnnouncement()
                    );
                    info.setMasterAddress(game.getMasterAddress()); // Устанавливаем адрес
                    gamesList.add(info);
                    Logger.info("Game added to lobby: {} at {}", game.getGameName(), game.getMasterAddress());
                });
        });

            // Подписываемся на удаление игр
            discoveryService.addGameRemovedListener(gameName -> {
                Platform.runLater(() -> {
                    gamesList.removeIf(g -> g.getName().equals(gameName)); // ← ИСПРАВЛЕНО
                    Logger.info("Game removed from lobby: {}", gameName);
                });
            });

            Logger.info("Discovery started in lobby");

        } catch (Exception e) {
            Logger.error("Failed to start discovery: {}", e.getMessage(), e);
            showError("Failed to start game discovery");
        }
    }


    @FXML
    private void onJoin() {
        GameInfo selected = gamesTable.getSelectionModel().getSelectedItem();
        if (selected == null) {
            showError("Please select a game to join");
            return;
        }

        String playerName = playerNameField.getText().trim();
        if (playerName.isEmpty()) {
            showError("Please enter your name");
            return;
        }

        if (!selected.getStatus().equals("Open")) {
            showError("This game is full");
            return;
        }

        try {
            // Определяем роль
            NodeRole role = playRadio.isSelected() ? NodeRole.NORMAL : NodeRole.VIEWER;

            // Получаем РЕАЛЬНЫЙ адрес мастера из discovery
            InetSocketAddress masterAddress = selected.getMasterAddress(); // ← ИСПРАВЛЕНО

            if (masterAddress == null) {
                showError("Cannot determine master address");
                return;
            }

            Logger.info("Connecting to master at {}", masterAddress);

            // Создаем GameService и присоединяемся
            gameService = new GameService();
            gameService.joinGame(
                    selected.getAnnouncement(),
                    playerName,
                    role,
                    masterAddress
            );

            Logger.info("Joined game: {}", selected.getName());

            // Останавливаем discovery
            discoveryService.stop();

            // Получаем текущее окно lobby
            Stage lobbyStage = (Stage) joinButton.getScene().getWindow();

            // Открываем игровое окно
            openGameWindow();

            // Закрываем lobby
            lobbyStage.close();

        } catch (Exception e) {
            Logger.error("Failed to join game: {}", e.getMessage(), e);
            showError("Failed to join game: " + e.getMessage());
        }
    }

    private void openGameWindow() {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/game.fxml"));
            Parent root = loader.load();

            GameController controller = loader.getController();
            controller.setGameService(gameService);

            Stage stage = new Stage();
            stage.setTitle("Snake Game - " + gameService.getLocalPlayer().getName());
            stage.setScene(new Scene(root, 1000, 700));
            stage.setOnCloseRequest(event -> {
                controller.stop();
                gameService.leaveGame();

                // При закрытии игры - открываем главное меню
                openMainMenu();
            });
            stage.show();

        } catch (Exception e) {
            Logger.error("Failed to open game window: {}", e.getMessage(), e);
        }
    }

    private void openMainMenu() {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/main-menu.fxml"));
            Parent root = loader.load();

            Stage stage = new Stage();
            Scene scene = new Scene(root, 800, 600);
            scene.getStylesheets().add(getClass().getResource("/css/styles.css").toExternalForm());

            stage.setTitle("Snake Game - Multiplayer");
            stage.setScene(scene);
            stage.show();

        } catch (Exception e) {
            Logger.error("Failed to open main menu: {}", e.getMessage(), e);
        }
    }


    @FXML
    private void onBack() {
        if (discoveryService != null) {
            discoveryService.stop();
        }

        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/main-menu.fxml"));
            Parent root = loader.load();

            Stage stage = (Stage) backButton.getScene().getWindow();
            stage.setScene(new Scene(root, 800, 600));
            stage.setTitle("Snake Game - Main Menu");

        } catch (Exception e) {
            Logger.error("Failed to go back: {}", e.getMessage(), e);
        }
    }



    private void showError(String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Error");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }

    // Класс для отображения игры в таблице
    // Класс для отображения игры в таблице
    public static class GameInfo {
        private final String name;
        private final String size;
        private final int players;
        private final String status;
        private final SnakesProto.GameAnnouncement announcement;
        private InetSocketAddress masterAddress;

        public GameInfo(String name, String size, int players, String status,
                        SnakesProto.GameAnnouncement announcement) {
            this.name = name;
            this.size = size;
            this.players = players;
            this.status = status;
            this.announcement = announcement;
        }

        public String getName() { return name; }
        public String getSize() { return size; }
        public int getPlayers() { return players; }
        public String getStatus() { return status; }

        public SnakesProto.GameAnnouncement getAnnouncement() {
            return announcement;
        }

        public InetSocketAddress getMasterAddress() {
            return masterAddress;
        }

        public void setMasterAddress(InetSocketAddress masterAddress) {
            this.masterAddress = masterAddress;
        }
    }

}
