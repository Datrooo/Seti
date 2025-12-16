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

        // Обработка State (получение состояния от MASTER)
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
        Logger.info("DeputyNode.onStart() called"); // ← ДОБАВЬТЕ!

        // Запускаем отправку Ping мастеру
        startPingTask();
        Logger.info("Ping task started"); // ← ДОБАВЬТЕ!

        // Запускаем проверку таймаута мастера
        startMasterTimeoutChecker();
        Logger.info("Master timeout checker started"); // ← ДОБАВЬТЕ!

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
            // Становимся мастером
            promoteToMaster();
        }
    }

    /**
     * Запуск проверки таймаута мастера
     */
    private void startMasterTimeoutChecker() {
        int timeoutMs = context.getGameConfig().nodeTimeoutMs();

        Logger.info("startMasterTimeoutChecker: timeoutMs={}, interval={}ms",
                timeoutMs, timeoutMs / 2); // ← ДОБАВЬТЕ!

        scheduler.scheduleAtFixedRate(() -> {
            try {
                Logger.debug("Running checkMasterTimeout..."); // ← ДОБАВЬТЕ!
                checkMasterTimeout();
            } catch (Exception e) {
                Logger.error("Error checking master timeout: {}", e.getMessage(), e);
            }
        }, timeoutMs, timeoutMs / 2, TimeUnit.MILLISECONDS);

        Logger.info("Scheduled master timeout checker"); // ← ДОБАВЬТЕ!
    }

    /**
     * Проверка таймаута мастера через NetworkManager
     */
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

    /**
     * Повышение Deputy до Master
     */
    private void promoteToMaster() {
        Logger.info("Deputy promoting to MASTER");

        // Обновляем роль локального игрока
        context.getLocalPlayer().setRole(NodeRole.MASTER);
        context.setMasterAddress(null); // Мы теперь мастер

        // Рассылаем ПЕРЕД остановкой scheduler!
        broadcastNewMaster();

        // Теперь останавливаем DeputyNode
        this.stop();

        // Создаем MasterNode
        MasterNode masterNode = new MasterNode(context);
        masterNode.start();

        Logger.info("Successfully promoted to MASTER");
    }


    /**
     * Рассылка уведомления о новом мастере всем игрокам
     */
    private void broadcastNewMaster() {
        NetworkManager network = context.getNetworkManager();
        int myId = context.getLocalPlayer().getId();

        // Получаем всех peer'ов
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

    /**
     * Отправка направления движения мастеру
     */
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

    /**
     * Обработка StateMsg от мастера
     */
    private void handleState(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!message.hasState()) {
            return;
        }
        lastMasterActivity.set(System.currentTimeMillis());

        // Обновляем состояние игры
        GameState newState = StateSerializer.fromProto(
                message.getState().getState(),
                context.getGameConfig()
        );

        // Для MASTER - обновляем GameEngine
        if (context.getGameEngine() != null) {
            context.getGameEngine().setGameState(newState);
        }

        // Для всех - сохраняем в context
        context.setCurrentState(newState);

        // Отправляем ACK
        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
        context.getNetworkManager().updatePeerActivity(sender);

        Logger.debug("Received state order={} from master", newState.getStateOrder());
    }

    /**
     * Обработка RoleChangeMsg
     */
    private void handleRoleChangeMessage(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!message.hasRoleChange()) {
            return;
        }

        SnakesProto.GameMessage.RoleChangeMsg roleChange = message.getRoleChange();

        // Проверяем, это нам адресовано
        if (message.hasReceiverId() &&
                message.getReceiverId() == context.getLocalPlayer().getId()) {

            if (roleChange.hasReceiverRole()) {
                NodeRole newRole = StateSerializer.nodeRoleFromProto(roleChange.getReceiverRole());
                handleRoleChange(newRole, context.getMasterAddress());
            }
        }

        // Если отправитель становится мастером
        if (roleChange.hasSenderRole() &&
                roleChange.getSenderRole() == SnakesProto.NodeRole.MASTER) {
            context.setMasterAddress(sender);
            Logger.info("New master: {}", sender);
        }

        // Отправляем ACK
        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
        context.getNetworkManager().updatePeerActivity(sender);
    }

    /**
     * Периодическая отправка Ping мастеру
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
