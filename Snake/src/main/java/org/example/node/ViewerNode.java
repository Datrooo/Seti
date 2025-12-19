package org.example.node;

import org.example.game.model.GameState;
import org.example.game.serialization.StateSerializer;
import org.example.network.NetworkManager;
import org.example.protocol.SnakesProto;
import org.example.util.Logger;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ViewerNode extends Node {
    private final ScheduledExecutorService scheduler;
    private final AtomicLong lastMasterActivity = new AtomicLong(System.currentTimeMillis());
    private static final long MASTER_CHECK_INTERVAL_MS = 200;

    public ViewerNode(NodeContext context) {
        super(context, NodeRole.VIEWER);
        this.scheduler = Executors.newScheduledThreadPool(2);
    }

    @Override
    protected void registerMessageHandlers() {
        NetworkManager network = context.getNetworkManager();

        sub(network.getDispatcher().subscribeState(this::handleState));
        sub(network.getDispatcher().subscribeError(this::handleError));
        sub(network.getDispatcher().subscribePing(this::handlePing));
        sub(network.getDispatcher().subscribeAck(this::handleAck));
    }

    @Override
    protected void onStart() {
        startPingTask();
        startMasterTimeoutChecker();
        Logger.info("ViewerNode started, observing game at {}", context.getMasterAddress());
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
    }

    private void handleState(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!running) return;
        if (!message.hasState()) return;

        if (sender.equals(context.getMasterAddress())) {
            lastMasterActivity.set(System.currentTimeMillis());
        }

        // ВАЖНО: десериализовать state и сохранить в context
        GameState newState = StateSerializer.fromProto(
                message.getState().getState(),
                context.getGameConfig()
        );

        // ← это критично для viewer'а!
        context.setCurrentState(newState);

        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
        context.getNetworkManager().updatePeerActivity(sender);

        Logger.debug("Viewer received state order={}", newState.getStateOrder());
    }

    protected void handlePing(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!running) return;
        if (sender.equals(context.getMasterAddress())) {
            lastMasterActivity.set(System.currentTimeMillis());
        }
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

    private void startMasterTimeoutChecker() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                checkMasterTimeout();
            } catch (Exception e) {
                Logger.error("Error checking master timeout: {}", e.getMessage(), e);
            }
        }, MASTER_CHECK_INTERVAL_MS, MASTER_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void checkMasterTimeout() {
        InetSocketAddress master = context.getMasterAddress();
        if (master == null) return;

        int timeoutMs = context.getGameConfig().nodeTimeoutMs();
        long elapsed = System.currentTimeMillis() - lastMasterActivity.get();

        if (elapsed > timeoutMs) {
            Logger.warn("Master timed out for viewer, disconnecting");
            // Viewer может просто отписаться или оставить старое состояние
        }
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
        if (masterAddr == null) return;

        SnakesProto.GameMessage ping = org.example.game.serialization.MessageBuilder.createPing(
                context.getLocalPlayer().getId(),
                0
        );

        context.getNetworkManager().sendWithAck(ping, masterAddr);
    }
}
