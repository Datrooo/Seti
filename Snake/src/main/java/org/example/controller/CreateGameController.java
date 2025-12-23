package org.example.controller;

import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.stage.Stage;
import org.example.game.model.GameConfig;
import org.example.service.GameService;
import org.example.util.Logger;

public class CreateGameController {

    @FXML private TextField gameNameField;
    @FXML private TextField playerNameField;
    @FXML private Spinner<Integer> widthSpinner;
    @FXML private Spinner<Integer> heightSpinner;
    @FXML private Spinner<Integer> foodStaticSpinner;
    @FXML private Spinner<Integer> stateDelaySpinner;

    private GameService gameService;

    @FXML
    private void initialize() {
        widthSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(10, 100, 40));
        heightSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(10, 100, 30));
        foodStaticSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(0, 100, 1));
        stateDelaySpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(100, 3000, 1000, 100));

        gameService = new GameService();
    }

    @FXML
    private void onCreate() {
        String gameName = gameNameField.getText().trim();
        String playerName = playerNameField.getText().trim();

        if (gameName.isEmpty()) {
            showError("Game name cannot be empty");
            return;
        }

        if (playerName.isEmpty()) {
            showError("Player name cannot be empty");
            return;
        }

        try {
            GameConfig config = new GameConfig(
                    widthSpinner.getValue(),
                    heightSpinner.getValue(),
                    foodStaticSpinner.getValue(),
                    1,
                    stateDelaySpinner.getValue(),
                    0.1f,
                    1000,
                    3000
            );
            gameService.createGame(gameName, playerName, config);
            Logger.info("Game created: {}", gameName);
            Stage createDialog = (Stage) gameNameField.getScene().getWindow();
            openGameWindow();
            closeMainMenu();
            createDialog.close();

        } catch (Exception e) {
            Logger.error("Failed to create game: {}", e.getMessage(), e);
            showError("Failed to create game: " + e.getMessage());
        }
    }

    private void closeMainMenu() {
        javafx.stage.Window.getWindows().stream()
                .filter(window -> window instanceof Stage)
                .map(window -> (Stage) window)
                .filter(stage -> "Snake Game - Multiplayer".equals(stage.getTitle()))
                .findFirst()
                .ifPresent(Stage::close);
    }

    private void openGameWindow() {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/game.fxml"));
            Parent root = loader.load();

            GameController controller = loader.getController();
            controller.setGameService(gameService);

            Stage stage = new Stage();
            stage.setTitle("Snake Game - " + gameService.getLocalPlayer().getName());
            Scene scene = new Scene(root, 1000, 700);
            scene.getStylesheets().add(getClass().getResource("/css/styles.css").toExternalForm());
            stage.setScene(scene);
            stage.setOnCloseRequest(event -> {
                controller.stop();
                gameService.leaveGame();

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
    private void onCancel() {
        Stage stage = (Stage) gameNameField.getScene().getWindow();
        stage.close();
    }



    private void showError(String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Error");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }
}
