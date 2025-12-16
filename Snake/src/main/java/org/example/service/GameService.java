package org.example.service;

import org.example.game.model.Direction;
import org.example.game.model.GameConfig;
import org.example.game.model.GameState;
import org.example.game.model.Player;
import org.example.game.model.PlayerType;
import org.example.game.serialization.MessageBuilder;
import org.example.network.NetworkManager;
import org.example.node.*;
import org.example.protocol.SnakesProto;
import org.example.util.Config;
import org.example.util.IdGenerator;
import org.example.util.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Центральный сервис для управления игровой сессией
 */
public class GameService {
    private final AtomicReference<NetworkManager> networkManager;
    private final AtomicReference<Node> currentNode;
    private final AtomicReference<NodeContext> nodeContext;
    private volatile boolean active;

    public GameService() {
        this.networkManager = new AtomicReference<>(null);
        this.currentNode = new AtomicReference<>(null);
        this.nodeContext = new AtomicReference<>(null);
        this.active = false;
    }

    /**
     * Создает новую игру (становится MASTER)
     */
    public void createGame(String gameName, String playerName, GameConfig config) throws IOException {
        if (active) {
            throw new IllegalStateException("Game session already active");
        }

        Logger.info("Creating new game: {}", gameName);

        // Инициализируем сеть
        NetworkManager network = new NetworkManager(Config.DEFAULT_PORT);
        network.start();
        this.networkManager.set(network);

        // Создаем локального игрока
        int playerId = IdGenerator.generatePlayerId();
        Player localPlayer = new Player(
                playerId,
                playerName,
                null, // Адрес не нужен для локального игрока
                NodeRole.MASTER,
                PlayerType.HUMAN
        );

        // Создаем контекст
        NodeContext context = new NodeContext(network, localPlayer, gameName, config);
        this.nodeContext.set(context);

        // Создаем MasterNode
        MasterNode masterNode = new MasterNode(context);
        masterNode.start();
        this.currentNode.set(masterNode);

        active = true;
        Logger.info("Game created successfully as MASTER");
    }

    /**
     * Присоединяется к существующей игре
     */
    /**
     * Присоединяется к существующей игре
     */
    public void joinGame(
            SnakesProto.GameAnnouncement announcement,
            String playerName,
            NodeRole requestedRole,
            InetSocketAddress masterAddress) throws IOException {

        if (active) {
            throw new IllegalStateException("Game session already active");
        }

        Logger.info("Joining game: {}", announcement.getGameName());

        // Инициализируем сеть
        NetworkManager network = new NetworkManager(Config.DEFAULT_PORT);
        network.start();
        this.networkManager.set(network);

        // Создаем локального игрока (ID назначит мастер)
        int tempPlayerId = IdGenerator.generatePlayerId();
        Player localPlayer = new Player(
                tempPlayerId,
                playerName,
                null,
                requestedRole,
                PlayerType.HUMAN
        );

        // Получаем конфигурацию из announcement
        GameConfig config = org.example.game.serialization.StateSerializer.configFromProto(
                announcement.getConfig()
        );

        // Создаем контекст
        NodeContext context = new NodeContext(
                network,
                localPlayer,
                announcement.getGameName(),
                config
        );
        context.setMasterAddress(masterAddress);
        this.nodeContext.set(context);

        // НЕ регистрируем мастера заранее - пусть обработается через RoleChange

        // Создаем узел ПЕРЕД отправкой JOIN
        Node node = createNodeByRole(context, requestedRole);
        node.start(); // Запускаем - он подпишется на RoleChange
        this.currentNode.set(node);

        // Отправляем JoinMsg ОДИН РАЗ без повторений
        SnakesProto.GameMessage joinMsg = MessageBuilder.createJoin(
                playerName,
                announcement.getGameName(),
                requestedRole
        );

        // Используем простую отправку без ACK
        network.send(joinMsg, masterAddress);

        Logger.info("Join request sent to master at {}", masterAddress);

        active = true;
        Logger.info("Client started, waiting for role assignment");
    }



    /**
     * Отправляет команду управления змейкой
     */
    /**
     * Отправляет команду управления змейкой
     */
    public void steer(Direction direction) {
        Node node = currentNode.get();
        if (node == null) {
            Logger.warn("Cannot steer: no active node");
            return;
        }

        // Если мы MASTER - управляем напрямую через GameEngine
        if (node.getRole() == NodeRole.MASTER) {
            NodeContext context = nodeContext.get();
            if (context != null && context.getGameEngine() != null) {
                int playerId = context.getLocalPlayer().getId();
                context.getGameEngine().handleSteer(playerId, direction);
                Logger.debug("Master steered locally: {}", direction);
            }
            return;
        }

        // Для NORMAL и DEPUTY - отправляем SteerMsg мастеру
        if (node instanceof NormalNode normalNode) {
            normalNode.steer(direction);
        } else if (node instanceof DeputyNode deputyNode) {
            deputyNode.steer(direction);
        } else {
            Logger.warn("Cannot steer: current node is {}", node.getRole());
        }
    }


    /**
     * Получает текущее состояние игры
     */
    public GameState getGameState() {
        NodeContext context = nodeContext.get();
        if (context == null || context.getGameEngine() == null) {
            return null;
        }
        return context.getGameEngine().getGameState();
    }

    /**
     * Получает конфигурацию игры
     */
    public GameConfig getGameConfig() {
        NodeContext context = nodeContext.get();
        return context != null ? context.getGameConfig() : null;
    }

    public NodeContext getContext() {
        return nodeContext.get();
    }

    /**
     * Получает локального игрока
     */
    public Player getLocalPlayer() {
        NodeContext context = nodeContext.get();
        return context != null ? context.getLocalPlayer() : null;
    }

    /**
     * Получает текущую роль узла
     */
    public NodeRole getCurrentRole() {
        Node node = currentNode.get();
        return node != null ? node.getRole() : null;
    }

    /**
     * Переключает узел на новую роль (например, при повышении Deputy -> Master)
     */
    public void switchNode(NodeRole newRole) {
        Node oldNode = currentNode.get();
        if (oldNode == null) {
            return;
        }

        Logger.info("Switching node from {} to {}", oldNode.getRole(), newRole);

        // Останавливаем старый узел
        oldNode.stop();

        // Создаем новый узел
        NodeContext context = nodeContext.get();
        Node newNode = createNodeByRole(context, newRole);
        newNode.start();

        currentNode.set(newNode);
        Logger.info("Node switched successfully to {}", newRole);
    }

    /**
     * Выходит из игры
     */
    public void leaveGame() {
        if (!active) {
            return;
        }

        Logger.info("Leaving game");

        Node node = currentNode.get();
        if (node != null) {
            // Уведомляем мастера о выходе (отправляем RoleChange с sender_role = VIEWER)
            NodeContext context = nodeContext.get();
            if (context != null && context.getMasterAddress() != null) {
                SnakesProto.GameMessage roleChange = MessageBuilder.createRoleChange(
                        context.getLocalPlayer().getId(),
                        0,
                        NodeRole.VIEWER,
                        null
                );
                networkManager.get().sendWithAck(roleChange, context.getMasterAddress());
            }

            node.stop();
            currentNode.set(null);
        }

        NetworkManager network = networkManager.get();
        if (network != null) {
            network.stop();
            networkManager.set(null);
        }

        nodeContext.set(null);
        active = false;

        Logger.info("Left game successfully");
    }

    /**
     * Останавливает игровую сессию
     */
    public void shutdown() {
        leaveGame();
    }

    public boolean isActive() {
        return active;
    }

    public NetworkManager getNetworkManager() {
        return networkManager.get();
    }

    /**
     * Создает узел по роли
     */
    private Node createNodeByRole(NodeContext context, NodeRole role) {
        return switch (role) {
            case MASTER -> new MasterNode(context);
            case DEPUTY -> new DeputyNode(context);
            case NORMAL -> new NormalNode(context);
            case VIEWER -> new ViewerNode(context);
        };
    }
}
