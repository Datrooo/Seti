package org.example.node;

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

public class ViewerNode extends Node {
    private final ScheduledExecutorService scheduler;

    public ViewerNode(NodeContext context) {
        super(context, NodeRole.VIEWER);
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    @Override
    protected void registerMessageHandlers() {
        NetworkManager network = context.getNetworkManager();

        // Обработка State (только получение, не отправка)
        network.getDispatcher().onState(this::handleState);

        // Обработка RoleChange
        network.getDispatcher().onRoleChange(this::handleRoleChangeMessage);

        // Обработка Error
        network.getDispatcher().onError(this::handleError);

        // Обработка Ping
        network.getDispatcher().onPing(this::handlePing);
    }

    @Override
    protected void onStart() {
        // Запускаем отправку Ping мастеру
        startPingTask();

        Logger.info("ViewerNode started, observing game at {}", context.getMasterAddress());
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

        // Viewer обычно не меняет роль, но если это произошло...
        if (newRole != NodeRole.VIEWER) {
            Logger.info("Viewer role changed to {}, requires node recreation", newRole);
        }
    }

    /**
     * Обработка StateMsg от мастера
     */
    private void handleState(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!message.hasState()) {
            return;
        }

        // Обновляем состояние игры (только для отображения)
        GameState newState = StateSerializer.fromProto(
                message.getState().getState(),
                context.getGameConfig()
        );

        if (context.getGameEngine() != null) {
            context.getGameEngine().setGameState(newState);
        }

        // Отправляем ACK
        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
        context.getNetworkManager().updatePeerActivity(sender);

        Logger.debug("Viewer received state order={}", newState.getStateOrder());
    }

    /**
     * Обработка RoleChangeMsg
     */
    private void handleRoleChangeMessage(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!message.hasRoleChange()) {
            return;
        }

        SnakesProto.GameMessage.RoleChangeMsg roleChange = message.getRoleChange();

        // Если отправитель становится мастером, обновляем адрес
        if (roleChange.hasSenderRole() &&
                roleChange.getSenderRole() == SnakesProto.NodeRole.MASTER) {
            context.setMasterAddress(sender);
            Logger.info("New master for viewer: {}", sender);
        }

        // Отправляем ACK
        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
    }

    /**
     * Периодическая отправка Ping мастеру (чтобы нас не отключили)
     */
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
