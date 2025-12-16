package org.example.node;

import org.example.protocol.SnakesProto;
import org.example.util.Logger;

import java.net.InetSocketAddress;

/**
 * Базовый класс для всех типов узлов
 */
public abstract class Node {
    protected final NodeContext context;
    protected volatile NodeRole role;
    protected volatile boolean running;

    public Node(NodeContext context, NodeRole role) {
        this.context = context;
        this.role = role;
        this.running = false;
    }

    /**
     * Запускает узел
     */
    public void start() {
        if (running) {
            return;
        }

        running = true;
        registerMessageHandlers();
        onStart();

        Logger.info("{} node started for player {}",
                role, context.getLocalPlayer().getName());
    }

    /**
     * Останавливает узел
     */
    public void stop() {
        if (!running) {
            return;
        }

        running = false;
        onStop();

        Logger.info("{} node stopped", role);
    }

    /**
     * Регистрирует обработчики сообщений
     */
    protected abstract void registerMessageHandlers();

    /**
     * Вызывается при запуске узла
     */
    protected abstract void onStart();

    /**
     * Вызывается при остановке узла
     */
    protected abstract void onStop();

    /**
     * Обрабатывает изменение роли
     */
    public abstract void handleRoleChange(NodeRole newRole, InetSocketAddress newMasterAddress);

    /**
     * Обрабатывает Ping сообщение
     */
    protected void handlePing(SnakesProto.GameMessage message, InetSocketAddress sender) {
        context.getNetworkManager().updatePeerActivity(sender);
        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
    }

    /**
     * Обрабатывает Error сообщение
     */
    protected void handleError(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (message.hasError()) {
            String errorMsg = message.getError().getErrorMessage();
            Logger.error("Received error from {}: {}", sender, errorMsg);
            onError(errorMsg);
        }
    }

    /**
     * Вызывается при получении ошибки
     */
    protected void onError(String errorMessage) {
        // Переопределяется в подклассах
    }

    public NodeRole getRole() {
        return role;
    }

    public boolean isRunning() {
        return running;
    }

    public NodeContext getContext() {
        return context;
    }
}
