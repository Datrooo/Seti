package org.example.service;

import org.example.network.MulticastDiscovery;
import org.example.protocol.SnakesProto;
import org.example.util.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Сервис для поиска доступных игр через multicast
 */
public class DiscoveryService {
    private final MulticastDiscovery discovery;
    private final ConcurrentHashMap<String, GameInfo> availableGames;
    private final CopyOnWriteArrayList<Consumer<GameInfo>> gameAddedListeners;
    private final CopyOnWriteArrayList<Consumer<String>> gameRemovedListeners;
    private volatile boolean active;

    public DiscoveryService() {
        this.discovery = new MulticastDiscovery();
        this.availableGames = new ConcurrentHashMap<>();
        this.gameAddedListeners = new CopyOnWriteArrayList<>();
        this.gameRemovedListeners = new CopyOnWriteArrayList<>();
        this.active = false;
    }

    /**
     * Запускает поиск игр
     */
    public void start() throws IOException {
        if (active) {
            return;
        }

        discovery.start();

        // Подписываемся на объявления
        discovery.addAnnouncementListener(this::handleAnnouncement);

        // Запускаем проверку устаревших игр
        startTimeoutChecker();

        active = true;
        Logger.info("Discovery service started");
    }

    /**
     * Останавливает поиск игр
     */
    public void stop() {
        if (!active) {
            return;
        }

        discovery.stop();
        availableGames.clear();
        active = false;

        Logger.info("Discovery service stopped");
    }

    /**
     * Обрабатывает полученное объявление об игре
     */
    private void handleAnnouncement(MulticastDiscovery.AnnouncementWithAddress announcementWithAddress) {
        SnakesProto.GameAnnouncement announcement = announcementWithAddress.announcement();
        InetSocketAddress senderAddress = announcementWithAddress.senderAddress();

        String gameName = announcement.getGameName();

        GameInfo gameInfo = availableGames.get(gameName);
        boolean isNewGame = (gameInfo == null);

        if (isNewGame) {
            gameInfo = new GameInfo(announcement, senderAddress);
            availableGames.put(gameName, gameInfo);
            notifyGameAdded(gameInfo);
            Logger.info("Discovered new game: {} at {}", gameName, senderAddress);
        } else {
            // Обновляем существующую игру
            gameInfo.update(announcement, senderAddress);
        }
    }

    /**
     * Проверяет устаревшие игры (не обновлялись > 5 секунд)
     */
    private void startTimeoutChecker() {
        Thread checker = Thread.ofVirtual().start(() -> {
            while (active) {
                try {
                    Thread.sleep(2000);
                    checkTimeouts();
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
    }

    private void checkTimeouts() {
        long now = System.currentTimeMillis();
        long timeout = 5000; // 5 секунд

        availableGames.entrySet().removeIf(entry -> {
            GameInfo info = entry.getValue();
            if (now - info.getLastUpdate() > timeout) {
                notifyGameRemoved(entry.getKey());
                Logger.info("Game timed out: {}", entry.getKey());
                return true;
            }
            return false;
        });
    }

    /**
     * Получает список доступных игр
     */
    public Map<String, GameInfo> getAvailableGames() {
        return new ConcurrentHashMap<>(availableGames);
    }

    /**
     * Добавляет слушателя на появление новых игр
     */
    public void addGameAddedListener(Consumer<GameInfo> listener) {
        gameAddedListeners.add(listener);
    }

    /**
     * Добавляет слушателя на удаление игр
     */
    public void addGameRemovedListener(Consumer<String> listener) {
        gameRemovedListeners.add(listener);
    }

    private void notifyGameAdded(GameInfo gameInfo) {
        for (Consumer<GameInfo> listener : gameAddedListeners) {
            try {
                listener.accept(gameInfo);
            } catch (Exception e) {
                Logger.error("Error in game added listener: {}", e.getMessage());
            }
        }
    }

    private void notifyGameRemoved(String gameName) {
        for (Consumer<String> listener : gameRemovedListeners) {
            try {
                listener.accept(gameName);
            } catch (Exception e) {
                Logger.error("Error in game removed listener: {}", e.getMessage());
            }
        }
    }

    public boolean isActive() {
        return active;
    }

    /**
     * Информация об обнаруженной игре
     */
    public static class GameInfo {
        private SnakesProto.GameAnnouncement announcement;
        private InetSocketAddress masterAddress;
        private long lastUpdate;

        public GameInfo(SnakesProto.GameAnnouncement announcement, InetSocketAddress masterAddress) {
            this.announcement = announcement;
            this.masterAddress = masterAddress;
            this.lastUpdate = System.currentTimeMillis();
        }

        public void update(SnakesProto.GameAnnouncement newAnnouncement, InetSocketAddress newAddress) {
            this.announcement = newAnnouncement;
            this.masterAddress = newAddress;
            this.lastUpdate = System.currentTimeMillis();
        }

        public SnakesProto.GameAnnouncement getAnnouncement() {
            return announcement;
        }

        public String getGameName() {
            return announcement.getGameName();
        }

        public int getPlayerCount() {
            return announcement.getPlayers().getPlayersCount();
        }

        public boolean canJoin() {
            return announcement.hasCanJoin() && announcement.getCanJoin();
        }

        public int getFieldWidth() {
            return announcement.getConfig().hasWidth()
                    ? announcement.getConfig().getWidth() : 40;
        }

        public int getFieldHeight() {
            return announcement.getConfig().hasHeight()
                    ? announcement.getConfig().getHeight() : 30;
        }

        public long getLastUpdate() {
            return lastUpdate;
        }

        public InetSocketAddress getMasterAddress() {
            return masterAddress;
        }

        @Override
        public String toString() {
            return String.format("%s [%dx%d, %d players, %s] at %s",
                    getGameName(),
                    getFieldWidth(),
                    getFieldHeight(),
                    getPlayerCount(),
                    canJoin() ? "can join" : "full",
                    masterAddress
            );
        }
    }
}
