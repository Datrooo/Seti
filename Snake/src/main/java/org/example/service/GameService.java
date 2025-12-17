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

    public void createGame(String gameName, String playerName, GameConfig config) throws IOException {
        if (active) throw new IllegalStateException("Game session already active");

        Logger.info("Creating new game: {}", gameName);

        NetworkManager network = new NetworkManager(Config.DEFAULT_PORT);
        network.start();
        this.networkManager.set(network);

        int playerId = IdGenerator.generatePlayerId();
        Player localPlayer = new Player(
                playerId,
                playerName,
                null,
                NodeRole.MASTER,
                PlayerType.HUMAN
        );

        NodeContext context = new NodeContext(network, localPlayer, gameName, config);

        // чтобы requestNodeSwitch работал и у хоста тоже (если понадобится)
        context.setNodeChangeListener(this::switchNode);

        this.nodeContext.set(context);

        MasterNode masterNode = new MasterNode(context);
        masterNode.start();
        this.currentNode.set(masterNode);

        active = true;
        Logger.info("Game created successfully as MASTER");
    }

    public void joinGame(
            SnakesProto.GameAnnouncement announcement,
            String playerName,
            NodeRole requestedRole,
            InetSocketAddress masterAddress) throws IOException {

        if (active) throw new IllegalStateException("Game session already active");

        Logger.info("Joining game: {}", announcement.getGameName());

        NetworkManager network = new NetworkManager(0);
        network.start();
        this.networkManager.set(network);

        int tempPlayerId = 0;
        Player localPlayer = new Player(
                tempPlayerId,
                playerName,
                null,
                requestedRole,
                PlayerType.HUMAN
        );

        GameConfig config = org.example.game.serialization.StateSerializer.configFromProto(
                announcement.getConfig()
        );

        NodeContext context = new NodeContext(
                network,
                localPlayer,
                announcement.getGameName(),
                config
        );

        context.setMasterAddress(masterAddress);
        network.registerPeer(masterAddress, 0);

        // listener для switchNode() при смене роли/промоуте
        context.setNodeChangeListener(this::switchNode);

        this.nodeContext.set(context);

        Node node = createNodeByRole(context, requestedRole);
        node.start();
        this.currentNode.set(node);

        SnakesProto.GameMessage joinMsg = MessageBuilder.createJoin(
                tempPlayerId,
                playerName,
                announcement.getGameName(),
                requestedRole
        );
        network.sendWithAck(joinMsg, masterAddress);

        Logger.info("Join request sent to master at {}", masterAddress);

        active = true;
        Logger.info("Client started, waiting for role assignment");
    }

    public void steer(Direction direction) {
        Node node = currentNode.get();
        if (node == null) {
            Logger.warn("Cannot steer: no active node");
            return;
        }

        if (node.getRole() == NodeRole.MASTER) {
            NodeContext context = nodeContext.get();
            if (context != null && context.getGameEngine() != null) {
                int playerId = context.getLocalPlayer().getId();
                context.getGameEngine().handleSteer(playerId, direction);
                Logger.debug("Master steered locally: {}", direction);
            }
            return;
        }

        if (node instanceof NormalNode normalNode) {
            normalNode.steer(direction);
        } else if (node instanceof DeputyNode deputyNode) {
            deputyNode.steer(direction);
        } else {
            Logger.warn("Cannot steer: current node is {}", node.getRole());
        }
    }

    public GameState getGameState() {
        NodeContext context = nodeContext.get();
        if (context == null) return null;

        if (context.getGameEngine() != null) {
            return context.getGameEngine().getGameState();
        }
        return context.getCurrentState();
    }

    public GameConfig getGameConfig() {
        NodeContext context = nodeContext.get();
        return context != null ? context.getGameConfig() : null;
    }

    public NodeContext getContext() {
        return nodeContext.get();
    }

    public Player getLocalPlayer() {
        NodeContext context = nodeContext.get();
        return context != null ? context.getLocalPlayer() : null;
    }

    public NodeRole getCurrentRole() {
        Node node = currentNode.get();
        return node != null ? node.getRole() : null;
    }

    // ВАЖНО: под новую реализацию (ноды сами отписываются от dispatcher)
    public void switchNode(NodeRole newRole) {
        NodeContext context = nodeContext.get();
        if (context == null) return;

        Node oldNode = currentNode.get();
        if (oldNode == null) return;

        Logger.info("Switching node from {} to {}", oldNode.getRole(), newRole);

        // stop() должен отписать свои handlers (через Subscription tokens)
        oldNode.stop();

        // ВАЖНО: NetworkManager мог поменяться при promoteToMaster()
        this.networkManager.set(context.getNetworkManager());

        Node newNode = createNodeByRole(context, newRole);
        newNode.start();
        currentNode.set(newNode);

        Logger.info("Node switched successfully to {}", newRole);
    }

    public void leaveGame() {
        if (!active) return;

        Logger.info("Leaving game");

        NodeContext context = nodeContext.get();
        Node node = currentNode.get();

        if (node != null) {
            // (опционально) уведомление мастера — лучше адресно на masterAddress
            if (context != null && context.getMasterAddress() != null) {
                SnakesProto.GameMessage roleChange = MessageBuilder.createRoleChange(
                        context.getLocalPlayer().getId(),
                        context.getLocalPlayer().getId(), // адресат = я (или можно 0, если вы так договорились)
                        null,
                        NodeRole.VIEWER
                );
                context.getNetworkManager().sendWithAck(roleChange, context.getMasterAddress());
            }

            node.stop();
            currentNode.set(null);
        }

        if (context != null && context.getNetworkManager() != null) {
            context.getNetworkManager().stop();
        }
        networkManager.set(null);

        nodeContext.set(null);
        active = false;

        Logger.info("Left game successfully");
    }

    public void shutdown() {
        leaveGame();
    }

    public boolean isActive() {
        return active;
    }

    public NetworkManager getNetworkManager() {
        NodeContext ctx = nodeContext.get();
        return (ctx != null) ? ctx.getNetworkManager() : networkManager.get();
    }

    private Node createNodeByRole(NodeContext context, NodeRole role) {
        return switch (role) {
            case MASTER -> new MasterNode(context);
            case DEPUTY -> new DeputyNode(context);
            case NORMAL -> new NormalNode(context);
            case VIEWER -> new ViewerNode(context);
        };
    }
}
