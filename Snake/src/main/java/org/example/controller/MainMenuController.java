package org.example.controller;

import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.stage.Stage;
import org.example.util.Logger;

public class MainMenuController {

    @FXML private Button joinButton; // Добавили это

    @FXML
    private void onCreateGame() {
        try {
            Logger.info("Opening Create Game dialog");

            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/create-game.fxml"));
            Parent root = loader.load();

            Stage stage = new Stage();
            stage.setTitle("Create New Game");
            stage.setScene(new Scene(root, 400, 500));
            stage.show();

        } catch (Exception e) {
            Logger.error("Failed to open create game dialog: {}", e.getMessage(), e);
            showError("Failed to open create game dialog");
        }
    }

    @FXML
    private void onJoinGame() {
        try {
            Logger.info("Opening Lobby");

            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/lobby.fxml"));
            Parent root = loader.load();

            // Теперь можем получить Stage через joinButton
            Stage stage = (Stage) joinButton.getScene().getWindow();

            Scene scene = new Scene(root, 800, 600);
            stage.setScene(scene);
            stage.setTitle("Game Lobby");

        } catch (Exception e) {
            Logger.error("Failed to open lobby: {}", e.getMessage(), e);
            showError("Failed to open lobby");
        }
    }

    @FXML
    private void onExit() {
        Logger.info("Exiting application");
        System.exit(0);
    }

    private void showError(String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Error");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }
}
