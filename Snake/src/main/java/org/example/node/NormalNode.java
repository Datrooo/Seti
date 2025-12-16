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

public class NormalNode extends Node {
    private final ScheduledExecutorService scheduler;
    private volatile Direction pendingDirection;

    public NormalNode(NodeContext context) {
        super(context, NodeRole.NORMAL);
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.pendingDirection = null;
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
        startPingTask();
        Logger.info("NormalNode started, master at {}", context.getMasterAddress());
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

        // ← ДОБАВЛЕНО: Уведомляем GameService о смене роли
        if (newRole == NodeRole.DEPUTY) {
            Logger.info("Promoted to DEPUTY, notifying GameService to switch node");
            if (context.getNodeChangeListener() != null) {
                context.getNodeChangeListener().onNodeRoleChanged(newRole);
            }
        }
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

        GameState newState = StateSerializer.fromProto(
                message.getState().getState(),
                context.getGameConfig()
        );

        if (context.getGameEngine() != null) {
            context.getGameEngine().setGameState(newState);
        }

        // ← ДОБАВЛЕНО
        context.setCurrentState(newState);

        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
        context.getNetworkManager().updatePeerActivity(sender);

        Logger.debug("Received state order={}", newState.getStateOrder());
    }

    private void handleRoleChangeMessage(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!message.hasRoleChange()) {
            return;
        }

        SnakesProto.GameMessage.RoleChangeMsg roleChange = message.getRoleChange();

        Logger.info("Received RoleChange from {}", sender);

        if (message.hasReceiverId()) {
            int receiverId = message.getReceiverId();

            if (context.getLocalPlayer().getId() != receiverId) {
                Logger.info("Updating player ID from {} to {}",
                        context.getLocalPlayer().getId(), receiverId);
                context.getLocalPlayer().setId(receiverId);
            }

            int masterId = message.getSenderId();
            context.getNetworkManager().registerPeer(sender, masterId);
            Logger.info("Registered master {} with id {}", sender, masterId);

            if (roleChange.hasReceiverRole()) {
                NodeRole newRole = StateSerializer.nodeRoleFromProto(roleChange.getReceiverRole());
                handleRoleChange(newRole, context.getMasterAddress());
            }
        }

        if (roleChange.hasSenderRole() &&
                roleChange.getSenderRole() == SnakesProto.NodeRole.MASTER) {
            context.setMasterAddress(sender);
            Logger.info("New master: {}", sender);
        }

        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
        context.getNetworkManager().updatePeerActivity(sender);

        Logger.info("Role assignment complete, starting normal operations");
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
