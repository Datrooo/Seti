package org.example;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import org.example.util.Logger;

public class SnakesApplication extends Application {

    @Override
    public void start(Stage primaryStage) throws Exception {
        Logger.info("Starting Snakes Application");

        // Загружаем главное меню
        FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/main-menu.fxml"));
        Parent root = loader.load();

        Scene scene = new Scene(root, 800, 600);
        scene.getStylesheets().add(getClass().getResource("/css/styles.css").toExternalForm());

        primaryStage.setTitle("Snake Game - Multiplayer");
        primaryStage.setScene(scene);
        primaryStage.setOnCloseRequest(event -> {
            Logger.info("Application closing");
            System.exit(0);
        });

        primaryStage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
