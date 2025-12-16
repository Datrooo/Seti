package org.example.node;

import org.example.game.model.Direction;
import org.example.game.model.GameState;
import org.example.game.serialization.MessageBuilder;
import org.example.game.serialization.StateSerializer;
import org.example.network.NetworkManager;
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

        Logger.info("startMasterTimeoutChecker: timeoutMs={}, interval={}ms",
                timeoutMs, timeoutMs / 2);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                Logger.debug("Running checkMasterTimeout...");
                checkMasterTimeout();
            } catch (Exception e) {
                Logger.error("Error checking master timeout: {}", e.getMessage(), e);
            }
        }, timeoutMs, timeoutMs / 2, TimeUnit.MILLISECONDS);

        Logger.info("Scheduled master timeout checker");
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

        // Обновляем роль локального игрока
        context.getLocalPlayer().setRole(NodeRole.MASTER);
        context.setMasterAddress(null);

        // Рассылаем ПЕРЕД остановкой scheduler!
        broadcastNewMaster();

        // Теперь останавливаем DeputyNode
        this.stop();

        // Создаем MasterNode
        MasterNode masterNode = new MasterNode(context);
        masterNode.start();

        Logger.info("Successfully promoted to MASTER");
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

    private void handleRoleChangeMessage(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!message.hasRoleChange()) {
            return;
        }

        SnakesProto.GameMessage.RoleChangeMsg roleChange = message.getRoleChange();

        if (message.hasReceiverId() &&
                message.getReceiverId() == context.getLocalPlayer().getId()) {

            if (roleChange.hasReceiverRole()) {
                NodeRole newRole = StateSerializer.nodeRoleFromProto(roleChange.getReceiverRole());
                handleRoleChange(newRole, context.getMasterAddress());
            }
        }

        if (roleChange.hasSenderRole() &&
                roleChange.getSenderRole() == SnakesProto.NodeRole.MASTER) {
            context.setMasterAddress(sender);
            lastMasterActivity.set(System.currentTimeMillis());
            Logger.info("New master: {}", sender);
        }

        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
        context.getNetworkManager().updatePeerActivity(sender);
    }

    private void startPingTask() {
        int pingDelayMs = context.getGameConfig().pingDelayMs();

        scheduler.scheduleAtFixedRate(() -> {
            try {
                sendPingToMaster();
            } catch (Exception e) {
                Logger.error("Error sending ping: {}", e.getMessage());
            }
        }, pingDelayMs, pingDelayMs, TimeUnit.MILLISECONDS);
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
