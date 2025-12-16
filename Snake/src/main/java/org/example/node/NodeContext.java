package org.example.node;

import org.example.game.engine.GameEngine;
import org.example.game.model.GameConfig;
import org.example.game.model.Player;
import org.example.network.NetworkManager;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Общий контекст для всех узлов игры
 */
public class NodeContext {
    private final NetworkManager networkManager;
    private final Player localPlayer;
    private final String gameName;
    private final GameConfig gameConfig;
    private final AtomicReference<GameEngine> gameEngine;
    private final AtomicReference<InetSocketAddress> masterAddress;
    private final AtomicReference<InetSocketAddress> deputyAddress;

    public NodeContext(
            NetworkManager networkManager,
            Player localPlayer,
            String gameName,
            GameConfig gameConfig) {
        this.networkManager = networkManager;
        this.localPlayer = localPlayer;
        this.gameName = gameName;
        this.gameConfig = gameConfig;
        this.gameEngine = new AtomicReference<>(null);
        this.masterAddress = new AtomicReference<>(null);
        this.deputyAddress = new AtomicReference<>(null);
    }

    public NetworkManager getNetworkManager() {
        return networkManager;
    }

    public Player getLocalPlayer() {
        return localPlayer;
    }

    public String getGameName() {
        return gameName;
    }

    public GameConfig getGameConfig() {
        return gameConfig;
    }

    public GameEngine getGameEngine() {
        return gameEngine.get();
    }

    public void setGameEngine(GameEngine engine) {
        this.gameEngine.set(engine);
    }

    public InetSocketAddress getMasterAddress() {
        return masterAddress.get();
    }

    public void setMasterAddress(InetSocketAddress address) {
        this.masterAddress.set(address);
    }

    public InetSocketAddress getDeputyAddress() {
        return deputyAddress.get();
    }

    public void setDeputyAddress(InetSocketAddress address) {
        this.deputyAddress.set(address);
    }
}
