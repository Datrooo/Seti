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

        sub(network.getDispatcher().subscribeState(this::handleState));
        sub(network.getDispatcher().subscribeRoleChange(this::handleRoleChangeMessage));
        sub(network.getDispatcher().subscribeError(this::handleError));
        sub(network.getDispatcher().subscribePing(this::handlePing));
        sub(network.getDispatcher().subscribeAck(this::handleAck));
    }

    @Override
    protected void onStart() {
        startPingTask();
        startMasterTimeoutChecker();
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

        if (newRole == NodeRole.DEPUTY) {
            Logger.info("Promoted to DEPUTY, notifying GameService to switch node");
            if (context.getNodeChangeListener() != null) {
                context.getNodeChangeListener().onNodeRoleChanged(newRole);
            }
        }
    }

    public void steer(Direction direction) {
        if (!running) return;

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
        if (!message.hasRoleChange()) return;
        if (!message.hasReceiverId()) return;

        int myIdBefore = context.getLocalPlayer().getId();
        int receiverId = message.getReceiverId();
        int masterId = message.getSenderId();
        SnakesProto.GameMessage.RoleChangeMsg rc = message.getRoleChange();

        boolean isForMe = (myIdBefore == 0) || (myIdBefore == receiverId);
        if (!isForMe) {
            context.getNetworkManager().sendAck(message, sender, myIdBefore);
            return;
        }

        if (rc.hasSenderRole() && rc.getSenderRole() == SnakesProto.NodeRole.MASTER) {
            InetSocketAddress oldMaster = context.getMasterAddress();
            context.setMasterAddress(sender);
            lastMasterActivity.set(System.currentTimeMillis());
            context.getNetworkManager().redirectPeer(oldMaster, sender, masterId);
            Logger.info("New master: {}", sender);
        }

        if (myIdBefore == 0 && receiverId != 0) {
            Logger.info("Assigned playerId {} (was 0)", receiverId);
            context.getLocalPlayer().setId(receiverId);
        }

        context.getNetworkManager().registerPeer(sender, masterId);

        if (rc.hasReceiverRole()) {
            NodeRole newRole = StateSerializer.nodeRoleFromProto(rc.getReceiverRole());
            handleRoleChange(newRole, sender);
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
        if (masterAddr == null) return;

        SnakesProto.GameMessage ping = MessageBuilder.createPing(
                context.getLocalPlayer().getId(),
                0
        );

        context.getNetworkManager().sendWithAck(ping, masterAddr);
    }
}
