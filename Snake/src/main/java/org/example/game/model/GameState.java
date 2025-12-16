package org.example.game.model;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GameState {
    private final GameConfig config;
    private int stateOrder; // Номер состояния (увеличивается с каждым обновлением)
    private final Map<Integer, Snake> snakes; // playerId -> Snake
    private final Map<Integer, Player> players; // playerId -> Player
    private final Set<Coord> foods;

    public GameState(GameConfig config) {
        this.config = config;
        this.stateOrder = 0;
        this.snakes = new ConcurrentHashMap<>();
        this.players = new ConcurrentHashMap<>();
        this.foods = Collections.synchronizedSet(new HashSet<>());
    }

    // Глубокое копирование для безопасной передачи между потоками
    public GameState copy() {
        GameState copied = new GameState(config);

        this.players.values().forEach(copied::addPlayer);

        this.snakes.values().forEach(copied::addSnake);

        this.foods.forEach(copied::addFood);

        copied.stateOrder = this.stateOrder;

        return copied;
    }

    public void incrementStateOrder() {
        stateOrder++;
    }

    public void addPlayer(Player player) {
        players.put(player.getId(), player);
    }

    public void removePlayer(int playerId) {
        players.remove(playerId);
    }

    public void addSnake(Snake snake) {
        snakes.put(snake.getPlayerId(), snake);
    }

    public void removeSnake(int playerId) {
        snakes.remove(playerId);
    }

    public void addFood(Coord coord) {
        foods.add(coord);
    }

    public void removeFood(Coord coord) {
        foods.remove(coord);
    }

    public boolean isCellOccupied(Coord coord) {
        // Проверяем, занята ли клетка телом змейки
        for (Snake snake : snakes.values()) {
            if (snake.contains(coord)) {
                return true;
            }
        }
        return false;
    }

    public boolean isFoodAt(Coord coord) {
        return foods.contains(coord);
    }

    public Optional<Player> getPlayer(int playerId) {
        return Optional.ofNullable(players.get(playerId));
    }

    public Optional<Snake> getSnake(int playerId) {
        return Optional.ofNullable(snakes.get(playerId));
    }

    public Collection<Player> getPlayers() {
        return new ArrayList<>(players.values());
    }

    public Collection<Snake> getSnakes() {
        return new ArrayList<>(snakes.values());
    }

    public Set<Coord> getFoods() {
        return new HashSet<>(foods);
    }

    public GameConfig getConfig() {
        return config;
    }

    public int getStateOrder() {
        return stateOrder;
    }

    public int getPlayerCount() {
        return players.size();
    }

    public int getAlivePlayerCount() {
        return (int) snakes.values().stream()
                .filter(Snake::isAlive)
                .count();
    }

    public void setStateOrder(int order) {
        this.stateOrder = order;
    }
}
