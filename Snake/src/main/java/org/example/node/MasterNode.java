package org.example.node;

import org.example.game.engine.GameEngine;
import org.example.game.model.*;
import org.example.game.serialization.MessageBuilder;
import org.example.game.serialization.StateSerializer;
import org.example.network.NetworkManager;
import org.example.network.PeerInfo;
import org.example.protocol.SnakesProto;
import org.example.util.IdGenerator;
import org.example.util.Logger;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MasterNode extends Node {
    private final ScheduledExecutorService scheduler;
    private GameEngine gameEngine;
    private final java.util.concurrent.ConcurrentHashMap<Integer, Long> lastSteerSeq;

    public MasterNode(NodeContext context) {
        super(context, NodeRole.MASTER);
        this.scheduler = Executors.newScheduledThreadPool(3);
        this.lastSteerSeq = new java.util.concurrent.ConcurrentHashMap<>();

        if (context.getGameEngine() == null) {
            Logger.info("Creating new GameEngine for new game");
            this.gameEngine = new GameEngine(context.getGameConfig());
            context.setGameEngine(gameEngine);

            Snake snake = gameEngine.addPlayer(context.getLocalPlayer());
            Logger.info("Master created with snake at {}", snake.getHead());
        } else {
            Logger.info("Using existing GameEngine from Deputy promotion");
            this.gameEngine = context.getGameEngine();
            Logger.info("Game state preserved: order={}, players={}",
                    gameEngine.getGameState().getStateOrder(),
                    gameEngine.getGameState().getPlayerCount()
            );
        }
    }

    @Override
    protected void registerMessageHandlers() {
        NetworkManager network = context.getNetworkManager();

        sub(network.getDispatcher().subscribePing(this::handlePing));
        sub(network.getDispatcher().subscribeSteer(this::handleSteer));
        sub(network.getDispatcher().subscribeJoin(this::handleJoin));
        sub(network.getDispatcher().subscribeDiscover(this::handleDiscover));
    }

    private boolean hasDeputyInState() {
        int myId = context.getLocalPlayer().getId();
        for (Player p : gameEngine.getGameState().getPlayers()) {
            if (p.getRole() == NodeRole.DEPUTY && p.getId() != myId) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void onStart() {
        if (context.getGameEngine() == null) {
            gameEngine = new GameEngine(context.getGameConfig());

            int maxId = gameEngine.getGameState().getPlayers().stream()
                    .mapToInt(Player::getId)
                    .max()
                    .orElse(0);
            IdGenerator.setNextPlayerId(maxId + 1);
            Logger.info("Synced playerIdCounter to {}", maxId + 1);

            context.setGameEngine(gameEngine);

            Player localPlayer = context.getLocalPlayer();
            Snake snake = gameEngine.addPlayer(localPlayer);
            Logger.info("Player {} joined the game with snake at {}", localPlayer.getName(), snake.getHead());
        } else {
            gameEngine = context.getGameEngine();
            Logger.info("Using existing GameEngine from onStart()");
        }

        context.setMasterAddress(null);
        bootstrapPeersFromState();

        if (!hasDeputyInState()) {
            selectDeputy();
        }
        startGameLoop();
        startStateUpdates();
        startAnnouncements();
        startTimeoutChecker();

        Logger.info("MASTER node started for player {}", context.getLocalPlayer().getName());
    }

    @Override
    protected void onStop() {
        scheduler.shutdownNow();
    }

    @Override
    public void handleRoleChange(NodeRole newRole, InetSocketAddress newMasterAddress) {
        if (newRole == NodeRole.DEPUTY) {
            Logger.info("Stepping down from MASTER to DEPUTY");
        }
    }

    private void startGameLoop() {
        int stateDelayMs = context.getGameConfig().stateDelayMs();

        scheduler.scheduleAtFixedRate(() -> {
            try {
                gameEngine.update();
                GameState newState = gameEngine.getGameState();

                Snake mySnake = null;
                for (Snake snake : newState.getSnakes()) {
                    if (snake.getPlayerId() == context.getLocalPlayer().getId()) {
                        mySnake = snake;
                        break;
                    }
                }

                Logger.debug("After update: order={}, mySnake alive={}",
                        newState.getStateOrder(),
                        mySnake != null ? mySnake.isAlive() : "NOT FOUND");

                context.setCurrentState(newState);
            } catch (Exception e) {
                Logger.error("Error in game loop: {}", e.getMessage(), e);
            }
        }, 0, stateDelayMs, TimeUnit.MILLISECONDS);
    }

    private void startStateUpdates() {
        int stateDelayMs = context.getGameConfig().stateDelayMs();

        scheduler.scheduleAtFixedRate(() -> {
            try {
                broadcastState();
            } catch (Exception e) {
                Logger.error("Error broadcasting state: {}", e.getMessage(), e);
            }
        }, 0, stateDelayMs, TimeUnit.MILLISECONDS);
    }

    private void startTimeoutChecker() {
        int timeoutMs = context.getGameConfig().nodeTimeoutMs();
        long checkInterval = Math.max(100, timeoutMs / 2L);

        Logger.info("Starting timeout checker: timeoutMs={}, checkInterval={}ms", timeoutMs, checkInterval);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                context.getNetworkManager().checkPeerTimeouts(timeoutMs, this::handlePlayerTimeout);
            } catch (Exception e) {
                Logger.error("Error checking timeouts: {}", e.getMessage(), e);
            }
        }, timeoutMs, checkInterval, TimeUnit.MILLISECONDS);
    }

    private void broadcastState() {
        SnakesProto.GameMessage stateMsg = MessageBuilder.createState(
                gameEngine.getGameState(),
                context.getLocalPlayer().getId()
        );

        NetworkManager network = context.getNetworkManager();
        for (PeerInfo peer : network.getAllPeers()) {
            network.sendWithAck(stateMsg, peer.getAddress());
        }
    }

    private void startAnnouncements() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                announceGame();
            } catch (Exception e) {
                Logger.error("Error announcing game: {}", e.getMessage(), e);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    private void announceGame() {
        boolean canJoin = gameEngine.getGameState().getPlayerCount() < 10;
        SnakesProto.GameMessage announcement = MessageBuilder.createAnnouncement(
                context.getGameName(),
                context.getGameConfig(),
                gameEngine.getGameState(),
                canJoin
        );
        context.getNetworkManager().announceGame(announcement);
    }

    private void handlePlayerTimeout(PeerInfo peer) {
        Logger.warn("Player {} timed out, removing", peer.getPlayerId());

        InetSocketAddress deputy = context.getDeputyAddress();
        boolean wasDeputy = deputy != null && deputy.equals(peer.getAddress());

        Snake snake = gameEngine.getGameState().getSnakeByPlayerId(peer.getPlayerId());
        if (snake != null && snake.isAlive()) {
            snake.setState(Snake.SnakeState.ZOMBIE);
            Logger.info("Player {} left, snake became zombie", peer.getPlayerId());
        }

        gameEngine.removePlayer(peer.getPlayerId());
        context.getNetworkManager().unregisterPeer(peer.getAddress());
        
        lastSteerSeq.remove(peer.getPlayerId());
        
        if (wasDeputy) {
            Logger.warn("Deputy timed out, clearing deputyAddress");
            context.setDeputyAddress(null);
            selectDeputy();
        }

    }

    @Override
    protected void handlePing(SnakesProto.GameMessage message, InetSocketAddress sender) {
        Logger.debug("Received PING from {}", sender);
        context.getNetworkManager().updatePeerActivity(sender);
        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
    }

    private void handleSteer(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!message.hasSteer() || !message.hasSenderId()) return;

        Direction direction = StateSerializer.directionFromProto(message.getSteer().getDirection());
        int playerId = message.getSenderId();
        long msgSeq = message.getMsgSeq();

        Long lastSeq = lastSteerSeq.get(playerId);
        if (lastSeq != null && msgSeq <= lastSeq) {
            Logger.debug("Ignoring old/duplicate steer from player {}: seq={} (last={})", playerId, msgSeq, lastSeq);
            context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
            context.getNetworkManager().updatePeerActivity(sender);
            return;
        }

        lastSteerSeq.put(playerId, msgSeq);
        gameEngine.handleSteer(playerId, direction);
        Logger.debug("Accepted steer from player {}: direction={}, seq={}", playerId, direction, msgSeq);

        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
        context.getNetworkManager().updatePeerActivity(sender);
    }

    private void handleJoin(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!message.hasJoin()) return;

        SnakesProto.GameMessage.JoinMsg joinMsg = message.getJoin();
        Logger.info("[MASTER-JOIN] Received JOIN from {} (player: {}, msg_seq={})", 
                sender, joinMsg.getPlayerName(), message.getMsgSeq());

        for (Player existingPlayer : gameEngine.getGameState().getPlayers()) {
            if (sender.equals(existingPlayer.getAddress())) {
                Logger.warn("Player from {} already exists (id={}), sending ACK", sender, existingPlayer.getId());
                context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());

                SnakesProto.GameMessage roleChange = MessageBuilder.createRoleChange(
                        context.getLocalPlayer().getId(),
                        existingPlayer.getId(),
                        null,
                        existingPlayer.getRole()
                );
                context.getNetworkManager().sendWithAck(roleChange, sender);
                return;
            }
        }

        if (gameEngine.getGameState().getPlayerCount() >= 10) {
            sendError("Game is full", sender);
            return;
        }

        int newPlayerId = allocateUniquePlayerId();
        NodeRole requestedRole = StateSerializer.nodeRoleFromProto(joinMsg.getRequestedRole());
        PlayerType playerType = joinMsg.hasPlayerType()
                ? StateSerializer.playerTypeFromProto(joinMsg.getPlayerType())
                : PlayerType.HUMAN;

        Player newPlayer = new Player(
                newPlayerId,
                joinMsg.getPlayerName(),
                sender,
                requestedRole,
                playerType
        );

        context.getNetworkManager().registerPeer(sender, newPlayerId);

        if (requestedRole != NodeRole.VIEWER) {
            try {
                gameEngine.addPlayer(newPlayer);
                Logger.info("Player {} joined the game as {} with ID {}",
                        joinMsg.getPlayerName(), requestedRole, newPlayerId);

                if (context.getDeputyAddress() == null) {
                    selectDeputy();
                }
            } catch (IllegalStateException e) {
                Logger.warn("Cannot place snake for {}: {}", joinMsg.getPlayerName(), e.getMessage());
                context.getNetworkManager().unregisterPeer(sender);
                sendError("Cannot place snake: no suitable 5x5 square found on the field", sender);
                return;
            }
        } else {
            gameEngine.getGameState().addPlayer(newPlayer);
            Logger.info("Viewer {} connected with ID {} (no snake created)",
                    joinMsg.getPlayerName(), newPlayerId);
        }

        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
        Logger.info("[MASTER-JOIN] Sent ACK for JOIN seq={} to {} (playerId={})", 
                message.getMsgSeq(), sender, newPlayerId);

        SnakesProto.GameMessage roleChange = MessageBuilder.createRoleChange(
                context.getLocalPlayer().getId(),
                newPlayerId,
                null,
                requestedRole
        );
        context.getNetworkManager().sendWithAck(roleChange, sender);
        Logger.info("[MASTER-JOIN] Sent RoleChange to {}: role={}, id={}, seq={}", 
                sender, requestedRole, newPlayerId, roleChange.getMsgSeq());

        Logger.info("[MASTER-JOIN] Sent RoleChange to {}: role={}, id={}, seq={}", 
                sender, requestedRole, newPlayerId, roleChange.getMsgSeq());
    }


    private void handleDiscover(SnakesProto.GameMessage message, InetSocketAddress sender) {
        announceGame();
    }

    private void sendError(String errorMessage, InetSocketAddress destination) {
        SnakesProto.GameMessage error = MessageBuilder.createError(errorMessage, 0);
        context.getNetworkManager().send(error, destination);
    }

    private void selectDeputy() {
        for (Player player : gameEngine.getGameState().getPlayers()) {
            if (player.getRole() == NodeRole.NORMAL && player.getId() != context.getLocalPlayer().getId()) {
                player.setRole(NodeRole.DEPUTY);
                context.setDeputyAddress(player.getAddress());

                SnakesProto.GameMessage roleChange = MessageBuilder.createRoleChange(
                        context.getLocalPlayer().getId(),
                        player.getId(),
                        null,
                        NodeRole.DEPUTY
                );
                context.getNetworkManager().sendWithAck(roleChange, player.getAddress());

                Logger.info("Assigned {} as deputy", player.getName());
                break;
            }
        }
    }

    private void bootstrapPeersFromState() {
        NetworkManager network = context.getNetworkManager();
        int myId = context.getLocalPlayer().getId();

        for (Player p : gameEngine.getGameState().getPlayers()) {
            if (p.getId() == myId) continue;
            if (p.getAddress() != null) {
                network.registerPeer(p.getAddress(), p.getId());
            }
        }
    }

    private int allocateUniquePlayerId() {
        int id;
        while (true) {
            id = IdGenerator.generatePlayerId();
            boolean exists = gameEngine.getGameState().getPlayer(id).isPresent();
            if (!exists && id != context.getLocalPlayer().getId()) {
                return id;
            }
        }
    }
}
