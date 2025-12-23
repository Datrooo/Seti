package org.example.node;

import org.example.protocol.SnakesProto;
import org.example.util.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;


public abstract class Node {
    protected final NodeContext context;
    protected volatile NodeRole role;
    protected volatile boolean running;

    private final List<AutoCloseable> subscriptions = new ArrayList<>();

    public Node(NodeContext context, NodeRole role) {
        this.context = context;
        this.role = role;
        this.running = false;
    }

    protected final void sub(AutoCloseable s) {
        if (s != null) subscriptions.add(s);
    }

    public void start() {
        if (running) return;

        running = true;
        registerMessageHandlers();
        onStart();

        Logger.info("{} node started for player {}", role, context.getLocalPlayer().getName());
    }

    public void stop() {
        if (!running) return;

        running = false;

        for (AutoCloseable s : subscriptions) {
            try {
                s.close();
            } catch (Exception ignored) {
            }
        }
        subscriptions.clear();

        onStop();
        Logger.info("{} node stopped", role);
    }

    protected abstract void registerMessageHandlers();


    protected abstract void onStart();

    protected abstract void onStop();

    public abstract void handleRoleChange(NodeRole newRole, InetSocketAddress newMasterAddress);

    protected void handlePing(SnakesProto.GameMessage message, InetSocketAddress sender) {
        Logger.debug("Received PING from {}", sender);
        context.getNetworkManager().updatePeerActivity(sender);
        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
    }

    protected void handleError(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (message.hasError()) {
            String errorMsg = message.getError().getErrorMessage();
            Logger.error("Received error from {}: {}", sender, errorMsg);
            onError(errorMsg);
        }
    }

    protected void onError(String errorMessage) {
    }

    public NodeRole getRole() {
        return role;
    }

}
