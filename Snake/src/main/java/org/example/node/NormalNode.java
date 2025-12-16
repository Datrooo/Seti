package org.example.node;

import org.example.game.model.Direction;
import org.example.game.model.GameState;
import org.example.game.model.Player;
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

public class NormalNode extends Node {
    private final ScheduledExecutorService scheduler;
    private volatile Direction pendingDirection;
    private final AtomicLong lastMasterActivity = new AtomicLong(System.currentTimeMillis());
    private static final long MASTER_CHECK_INTERVAL_MS = 200;

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
        network.getDispatcher().onAck(this::handleAck);
    }
    @Override
    protected void onStart() {
        startPingTask();
        startMasterTimeoutChecker(); // FIX
        Logger.info("NormalNode started, master at {}", context.getMasterAddress());
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

        if (elapsed <= timeoutMs) return;

        InetSocketAddress deputyAddr = resolveDeputyAddressFromState();
        Integer deputyId = resolveDeputyIdFromState();

        if (deputyAddr == null || deputyId == null) {
            Logger.warn("Master timed out, but deputy is unknown (no state yet?)");
            return;
        }

        if (deputyAddr.equals(master)) return;

        Logger.warn("Master timed out ({}ms>{}ms), switching master {} -> deputy {}",
                elapsed, timeoutMs, master, deputyAddr);

        context.getNetworkManager().redirectPeer(master, deputyAddr, deputyId);
        context.setMasterAddress(deputyAddr);
        lastMasterActivity.set(System.currentTimeMillis());
    }

    private InetSocketAddress resolveDeputyAddressFromState() {
        if (context.getDeputyAddress() != null) return context.getDeputyAddress();
        GameState st = context.getCurrentState();
        if (st == null) return null;

        for (Player p : st.getPlayers()) {
            if (p.getRole() == NodeRole.DEPUTY && p.getAddress() != null) {
                context.setDeputyAddress(p.getAddress());
                return p.getAddress();
            }
        }
        return null;
    }

    private Integer resolveDeputyIdFromState() {
        GameState st = context.getCurrentState();
        if (st == null) return null;

        for (Player p : st.getPlayers()) {
            if (p.getRole() == NodeRole.DEPUTY) return p.getId();
        }
        return null;
    }

    private void handleState(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!running) return;
        if (!message.hasState()) return;

        // FIX: активность мастера
        if (sender.equals(context.getMasterAddress())) {
            lastMasterActivity.set(System.currentTimeMillis());
        }

        GameState newState = StateSerializer.fromProto(message.getState().getState(), context.getGameConfig());
        if (context.getGameEngine() != null) context.getGameEngine().setGameState(newState);
        context.setCurrentState(newState);

        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
        context.getNetworkManager().updatePeerActivity(sender);
        Logger.debug("Received state order={}", newState.getStateOrder());
    }

    public void handlePing(SnakesProto.GameMessage message, InetSocketAddress sender) {
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


    private void handleRoleChangeMessage(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!running) return;
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

        if (roleChange.hasSenderRole() && roleChange.getSenderRole() == SnakesProto.NodeRole.MASTER) {
            InetSocketAddress old = context.getMasterAddress();
            context.setMasterAddress(sender);
            lastMasterActivity.set(System.currentTimeMillis());

            // FIX: перенос pending на нового мастера
            context.getNetworkManager().redirectPeer(old, sender, message.getSenderId());
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
