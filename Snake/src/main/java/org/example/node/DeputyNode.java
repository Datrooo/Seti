package org.example.node;

import org.example.game.engine.GameEngine;
import org.example.game.model.Direction;
import org.example.game.model.GameState;
import org.example.game.model.Snake;
import org.example.game.serialization.MessageBuilder;
import org.example.game.serialization.StateSerializer;
import org.example.network.NetworkManager;
import org.example.network.PeerInfo;
import org.example.protocol.SnakesProto;
import org.example.util.Logger;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class DeputyNode extends Node {
    private final ScheduledExecutorService scheduler;
    private volatile Direction pendingDirection;
    private final AtomicLong lastMasterActivity;
    private static final long MASTER_CHECK_INTERVAL_MS = 200;


    public DeputyNode(NodeContext context) {
        super(context, NodeRole.DEPUTY);
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.pendingDirection = null;
        this.lastMasterActivity = new AtomicLong(System.currentTimeMillis());
    }

    @Override
    protected void registerMessageHandlers() {
        NetworkManager network = context.getNetworkManager();

        network.getDispatcher().onState(this::handleState);
        network.getDispatcher().onRoleChange(this::handleRoleChangeMessage);
        network.getDispatcher().onError(this::handleError);
        network.getDispatcher().onPing(this::handlePing);
        network.getDispatcher().onAck(this::handleAck);

    }

    @Override
    protected void onStart() {
        Logger.info("DeputyNode.onStart() called");

        startPingTask();
        Logger.info("Ping task started");

        startMasterTimeoutChecker();
        Logger.info("Master timeout checker started");

        Logger.info("DeputyNode started, monitoring master at {}", context.getMasterAddress());
    }

    @Override
    protected void onStop() {
        scheduler.shutdownNow();
    }

    @Override
    public void handleRoleChange(NodeRole newRole, InetSocketAddress newMasterAddress) {
        if (!running) return;

        Logger.info("Role changed from {} to {}", role, newRole);
        this.role = newRole;
        context.getLocalPlayer().setRole(newRole);

        if (newMasterAddress != null) {
            context.setMasterAddress(newMasterAddress);
        }

        if (newRole == NodeRole.MASTER) {
            promoteToMaster();
        }
    }

    private void startMasterTimeoutChecker() {
        int timeoutMs = context.getGameConfig().nodeTimeoutMs();
        Logger.info("startMasterTimeoutChecker: timeoutMs={}, checkInterval={}ms", timeoutMs, MASTER_CHECK_INTERVAL_MS);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                checkMasterTimeout();
            } catch (Exception e) {
                Logger.error("Error checking master timeout: {}", e.getMessage(), e);
            }
        }, MASTER_CHECK_INTERVAL_MS, MASTER_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void checkMasterTimeout() {
        long now = System.currentTimeMillis();
        long lastActivity = lastMasterActivity.get();
        long elapsed = now - lastActivity;

        int timeoutMs = context.getGameConfig().nodeTimeoutMs();
        Logger.debug("Master timeout check: elapsed={}ms, limit={}ms", elapsed, timeoutMs);

        if (elapsed > timeoutMs) {
            Logger.warn("Master timed out! Elapsed: {}ms, promoting to MASTER", elapsed);
            promoteToMaster();
        }
    }

    private void promoteToMaster() {
        Logger.info("Deputy promoting to MASTER");

        GameState currentState = context.getCurrentState();
        InetSocketAddress oldMasterAddr = context.getMasterAddress();

        Integer oldMasterId = null;
        if (oldMasterAddr != null) {
            for (PeerInfo peer : context.getNetworkManager().getAllPeers()) {
                if (peer.getAddress().equals(oldMasterAddr)) {
                    oldMasterId = peer.getPlayerId();
                    Logger.info("Found old master: playerId={}, address={}", oldMasterId, oldMasterAddr);
                    break;
                }
            }
            context.getNetworkManager().unregisterPeer(oldMasterAddr);
            Logger.info("Removed dead master peer {}", oldMasterAddr);
        }

        context.getLocalPlayer().setRole(NodeRole.MASTER);

        if (context.getGameEngine() == null && currentState != null) {
            Logger.info("Creating GameEngine with current state");
            GameEngine gameEngine = new GameEngine(context.getGameConfig());
            gameEngine.setGameState(currentState);
            context.setGameEngine(gameEngine);

            // FIX: старый мастер -> ZOMBIE, не setAlive(false)
            if (oldMasterId != null) {
                Snake deadSnake = gameEngine.getGameState().getSnakeByPlayerId(oldMasterId);
                if (deadSnake != null && deadSnake.isAlive()) {
                    deadSnake.setState(Snake.SnakeState.ZOMBIE);
                    Logger.info("Dead master {} snake became zombie", oldMasterId);
                }
            }
        }

        context.setMasterAddress(null);
        broadcastNewMaster();
        this.stop();

        MasterNode masterNode = new MasterNode(context);
        masterNode.start();

        Logger.info("Successfully promoted to MASTER");
    }

    private void handleRoleChangeMessage(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!running) return;
        if (!message.hasRoleChange()) return;

        SnakesProto.GameMessage.RoleChangeMsg roleChange = message.getRoleChange();

        if (roleChange.hasSenderRole() && roleChange.getSenderRole() == SnakesProto.NodeRole.MASTER) {
            InetSocketAddress old = context.getMasterAddress();
            context.setMasterAddress(sender);
            lastMasterActivity.set(System.currentTimeMillis());

            // FIX: перенаправить pending-сообщения на нового мастера
            context.getNetworkManager().redirectPeer(old, sender, message.getSenderId());
            Logger.info("New master: {}", sender);
        }

        // остальное оставьте как было:
        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
        context.getNetworkManager().updatePeerActivity(sender);
    }

    private void handleAck(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!running) return;

        if (sender.equals(context.getMasterAddress())) {
            lastMasterActivity.set(System.currentTimeMillis());
        }
        context.getNetworkManager().updatePeerActivity(sender);
    }




    private void broadcastNewMaster() {
        NetworkManager network = context.getNetworkManager();
        int myId = context.getLocalPlayer().getId();

        for (var peer : network.getAllPeers()) {
            SnakesProto.GameMessage roleChange = MessageBuilder.createRoleChange(
                    myId,
                    peer.getPlayerId(),
                    NodeRole.MASTER,
                    null
            );

            network.sendWithAck(roleChange, peer.getAddress());
        }

        Logger.info("Broadcasted new master role to all players");
    }

    public void steer(Direction direction) {
        if (!running) {
            return;
        }

        InetSocketAddress masterAddr = context.getMasterAddress();
        if (masterAddr == null) {
            Logger.warn("Cannot steer: master address unknown");
            return;
        }

        this.pendingDirection = direction;

        SnakesProto.GameMessage steerMsg = MessageBuilder.createSteer(
                context.getLocalPlayer().getId(),
                direction
        );

        context.getNetworkManager().sendWithAck(steerMsg, masterAddr);
        Logger.debug("Sent steer: {}", direction);
    }

    private void handleState(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!running) return;

        if (!message.hasState()) {
            return;
        }

        lastMasterActivity.set(System.currentTimeMillis());

        GameState newState = StateSerializer.fromProto(
                message.getState().getState(),
                context.getGameConfig()
        );

        if (context.getGameEngine() != null) {
            context.getGameEngine().setGameState(newState);
        }

        context.setCurrentState(newState);

        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
        context.getNetworkManager().updatePeerActivity(sender);

        Logger.debug("Received state order={} from master", newState.getStateOrder());
    }

    private void startPingTask() {
        int pingDelayMs = context.getGameConfig().pingDelayMs();
        int timeoutMs = context.getGameConfig().nodeTimeoutMs();

        // чтобы не было ситуации pingDelay > timeout
        int effectivePing = Math.max(1, Math.min(pingDelayMs, Math.max(1, timeoutMs / 2)));

        scheduler.scheduleAtFixedRate(() -> {
            try {
                sendPingToMaster();
            } catch (Exception e) {
                Logger.error("Error sending ping: {}", e.getMessage());
            }
        }, effectivePing, effectivePing, TimeUnit.MILLISECONDS);
    }


    private void sendPingToMaster() {
        InetSocketAddress masterAddr = context.getMasterAddress();
        if (masterAddr == null) {
            return;
        }

        SnakesProto.GameMessage ping = MessageBuilder.createPing(
                context.getLocalPlayer().getId(),
                0
        );

        context.getNetworkManager().sendWithAck(ping, masterAddr);
    }
}
