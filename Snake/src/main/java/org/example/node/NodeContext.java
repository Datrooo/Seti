package org.example.node;

import org.example.game.engine.GameEngine;
import org.example.game.model.GameConfig;
import org.example.game.model.GameState;
import org.example.game.model.Player;
import org.example.network.NetworkManager;

import java.net.InetSocketAddress;

public class NodeContext {
    private  NetworkManager networkManager;
    private final Player localPlayer;
    private final String gameName;
    private final GameConfig gameConfig;
    private GameEngine gameEngine;
    private InetSocketAddress masterAddress;
    private InetSocketAddress deputyAddress;
    private volatile GameState currentState;
    private NodeChangeListener nodeChangeListener;

    public NodeContext(NetworkManager networkManager, Player localPlayer, String gameName, GameConfig gameConfig) {
        this.networkManager = networkManager;
        this.localPlayer = localPlayer;
        this.gameName = gameName;
        this.gameConfig = gameConfig;
        this.gameEngine = null;
        this.masterAddress = null;
        this.deputyAddress = null;
        this.currentState = null;
        this.nodeChangeListener = null;
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
        return gameEngine;
    }

    public void setGameEngine(GameEngine gameEngine) {
        this.gameEngine = gameEngine;
    }

    public InetSocketAddress getMasterAddress() {
        return masterAddress;
    }

    public void setMasterAddress(InetSocketAddress masterAddress) {
        this.masterAddress = masterAddress;
    }

    public InetSocketAddress getDeputyAddress() {
        return deputyAddress;
    }

    public void setDeputyAddress(InetSocketAddress deputyAddress) {
        this.deputyAddress = deputyAddress;
    }

    public GameState getCurrentState() {
        return currentState;
    }

    public void setCurrentState(GameState state) {
        this.currentState = state;
    }

    public NodeChangeListener getNodeChangeListener() {
        return nodeChangeListener;
    }

    public void setNodeChangeListener(NodeChangeListener listener) {
        this.nodeChangeListener = listener;
    }

    public void requestNodeSwitch(NodeRole newRole) {
        if (nodeChangeListener != null) {
            nodeChangeListener.onNodeRoleChanged(newRole);
        }
    }
    public void setNetworkManager(NetworkManager nm) {
        this.networkManager = nm;
    }


}
