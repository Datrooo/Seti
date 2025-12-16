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

    public MasterNode(NodeContext context) {
        super(context, NodeRole.MASTER);
        this.scheduler = Executors.newScheduledThreadPool(3);

        // Проверяем, есть ли уже GameEngine (при повышении Deputy)
        if (context.getGameEngine() == null) {
            // ✅ НОВЫЙ мастер - создаем новый GameEngine
            Logger.info("Creating new GameEngine for new game");
            this.gameEngine = new GameEngine(context.getGameConfig());
            context.setGameEngine(gameEngine);

            // Добавляем локального игрока только для НОВОЙ игры
            Snake snake = gameEngine.addPlayer(context.getLocalPlayer());
            Logger.info("Master created with snake at {}", snake.getHead());
        } else {
            // ✅ ПОВЫШЕНИЕ Deputy→Master - используем существующий GameEngine
            Logger.info("Using existing GameEngine from Deputy promotion");
            this.gameEngine = context.getGameEngine();

            // НЕ добавляем игрока - он уже есть в состоянии!
            Logger.info("Game state preserved: order={}, players={}",
                    gameEngine.getGameState().getStateOrder(),
                    gameEngine.getGameState().getPlayerCount());
        }
    }



    @Override
    protected void registerMessageHandlers() {
        NetworkManager network = context.getNetworkManager();

        // Обработка Ping
        network.getDispatcher().onPing(this::handlePing);

        // Обработка Steer (команды управления)
        network.getDispatcher().onSteer(this::handleSteer);

        // Обработка Join (новый игрок)
        network.getDispatcher().onJoin(this::handleJoin);

        // Обработка Discover
        network.getDispatcher().onDiscover(this::handleDiscover);
    }

    @Override
    protected void onStart() {
        // ✅ ПРОВЕРЯЕМ: Если GameEngine уже создан (Deputy promotion), не перезаписываем!
        if (context.getGameEngine() == null) {
            // Только для НОВОЙ игры
            gameEngine = new GameEngine(context.getGameConfig());
            context.setGameEngine(gameEngine);

            // Добавляем себя как игрока
            Player localPlayer = context.getLocalPlayer();
            Snake snake = gameEngine.addPlayer(localPlayer);
            Logger.info("Player {} joined the game with snake at {}",
                    localPlayer.getName(),
                    snake.getHead());
        } else {
            // Для Deputy promotion - используем существующий
            gameEngine = context.getGameEngine();
            Logger.info("Using existing GameEngine from onStart()");
        }

        context.setMasterAddress(null); // Мы сами мастер

        // Запускаем игровой цикл
        startGameLoop();

        // Запускаем рассылку состояния
        startStateUpdates();

        // Запускаем multicast объявления
        startAnnouncements();

        // Запускаем проверку таймаутов
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
            // Понижение с MASTER до DEPUTY
            Logger.info("Stepping down from MASTER to DEPUTY");
            // Нужно создать DeputyNode и переключиться
        }
    }

    /**
     * Игровой цикл - обновление состояния игры
     */
    private void startGameLoop() {
        int stateDelayMs = context.getGameConfig().stateDelayMs();

        scheduler.scheduleAtFixedRate(() -> {
            try {
                gameEngine.update();

                // ✅ ИСПРАВЛЕНО: Ищем змею через цикл
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

                // ✅ КРИТИЧНО: Обновляем context.currentState для UI!
                context.setCurrentState(newState);

            } catch (Exception e) {
                Logger.error("Error in game loop: {}", e.getMessage(), e);
            }
        }, stateDelayMs, stateDelayMs, TimeUnit.MILLISECONDS);
    }



    /**
     * Рассылка состояния игры всем игрокам
     */
    private void startStateUpdates() {
        int stateDelayMs = context.getGameConfig().stateDelayMs();

        scheduler.scheduleAtFixedRate(() -> {
            try {
                broadcastState();
            } catch (Exception e) {
                Logger.error("Error broadcasting state: {}", e.getMessage(), e);
            }
        }, stateDelayMs, stateDelayMs, TimeUnit.MILLISECONDS);
    }

    private void broadcastState() {
        SnakesProto.GameMessage stateMsg = MessageBuilder.createState(
                gameEngine.getGameState(),
                context.getLocalPlayer().getId()
        );

        // Отправляем всем зарегистрированным peer'ам
        NetworkManager network = context.getNetworkManager();
        for (PeerInfo peer : network.getAllPeers()) {
            network.sendWithAck(stateMsg, peer.getAddress());
        }
    }

    /**
     * Multicast объявления о игре
     */
    private void startAnnouncements() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                announceGame();
            } catch (Exception e) {
                Logger.error("Error announcing game: {}", e.getMessage(), e);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS); // Каждую секунду
    }

    private void announceGame() {
        boolean canJoin = gameEngine.getGameState().getPlayerCount() < 10; // Макс 10 игроков

        SnakesProto.GameMessage announcement = MessageBuilder.createAnnouncement(
                context.getGameName(),
                context.getGameConfig(),
                gameEngine.getGameState(),
                canJoin
        );

        context.getNetworkManager().announceGame(announcement);
    }

    /**
     * Проверка таймаутов игроков
     */
    private void startTimeoutChecker() {
        int timeoutMs = context.getGameConfig().nodeTimeoutMs();

        Logger.info("Starting timeout checker: timeoutMs={}, checkInterval={}ms",
                timeoutMs, timeoutMs / 2);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                Logger.debug("Checking peer timeouts with limit={}ms", timeoutMs);
                context.getNetworkManager().checkPeerTimeouts(timeoutMs, this::handlePlayerTimeout);
            } catch (Exception e) {
                Logger.error("Error checking timeouts: {}", e.getMessage(), e);
            }
        }, timeoutMs * 2L, timeoutMs / 2, TimeUnit.MILLISECONDS); // ← ОТЛОЖИТЬ первую проверку!
        //  ^^^^^^^^^^^^ Вместо timeoutMs дать больше времени на переход
    }



    private void handlePlayerTimeout(PeerInfo peer) {
        Logger.warn("Player {} timed out, removing", peer.getPlayerId());

        // ✅ КРИТИЧНО: Делаем змею зомби ПЕРЕД удалением игрока!
        Snake snake = gameEngine.getGameState().getSnakeByPlayerId(peer.getPlayerId());
        if (snake != null && snake.isAlive()) {
            snake.setAlive(false);
            Logger.info("Player {} left, snake became zombie", peer.getPlayerId());
        }

        gameEngine.removePlayer(peer.getPlayerId());
        context.getNetworkManager().unregisterPeer(peer.getAddress());
    }


    /**
     * Обработка Ping сообщения
     */
    protected void handlePing(SnakesProto.GameMessage message, InetSocketAddress sender) {
        Logger.debug("Received PING from {}", sender);

        // ВАЖНО: Обновляем активность отправителя
        context.getNetworkManager().updatePeerActivity(sender);

        // Отправляем ACK
        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
    }

    /**
     * Обработка команды управления змейкой
     */
    private void handleSteer(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!message.hasSteer() || !message.hasSenderId()) {
            return;
        }

        Direction direction = StateSerializer.directionFromProto(message.getSteer().getDirection());
        int playerId = message.getSenderId();

        gameEngine.handleSteer(playerId, direction);

        // Отправляем ACK
        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());
        context.getNetworkManager().updatePeerActivity(sender);
    }

    /**
     * Обработка запроса на присоединение
     */
    private void handleJoin(SnakesProto.GameMessage message, InetSocketAddress sender) {
        if (!message.hasJoin()) {
            return;
        }

        SnakesProto.GameMessage.JoinMsg joinMsg = message.getJoin();

        Logger.info("Received JOIN request from {} (player: {})", sender, joinMsg.getPlayerName());

        // Проверяем, может игрок с этого адреса уже существует
        for (Player existingPlayer : gameEngine.getGameState().getPlayers()) {
            if (sender.equals(existingPlayer.getAddress())) {
                Logger.warn("Player from {} already exists (id={}), sending ACK",
                        sender, existingPlayer.getId());

                // Отправляем ACK чтобы клиент перестал повторять JOIN
                context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());

                // Отправляем RoleChange еще раз
                SnakesProto.GameMessage roleChange = MessageBuilder.createRoleChange(
                        context.getLocalPlayer().getId(),
                        existingPlayer.getId(),
                        null,
                        existingPlayer.getRole()
                );
                context.getNetworkManager().sendWithAck(roleChange, sender);

                return; // НЕ создаем дубликат!
            }
        }

        // Проверяем, можно ли присоединиться
        if (gameEngine.getGameState().getPlayerCount() >= 10) {
            sendError("Game is full", sender);
            return;
        }

        // Создаем нового игрока
        int newPlayerId = IdGenerator.generatePlayerId();
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

        // Регистрируем peer ПЕРЕД добавлением в игру
        context.getNetworkManager().registerPeer(sender, newPlayerId);

        // Добавляем в игру
        gameEngine.addPlayer(newPlayer);

        Logger.info("Player {} joined the game as {} with ID {}",
                joinMsg.getPlayerName(), requestedRole, newPlayerId);

        // Отправляем ACK на JoinMsg
        context.getNetworkManager().sendAck(message, sender, context.getLocalPlayer().getId());

        // Отправляем RoleChangeMsg с назначенным ID и ролью
        SnakesProto.GameMessage roleChange = MessageBuilder.createRoleChange(
                context.getLocalPlayer().getId(),
                newPlayerId,
                null,
                requestedRole
        );
        context.getNetworkManager().sendWithAck(roleChange, sender);

        Logger.info("Sent role assignment to {}: role={}, id={}", sender, requestedRole, newPlayerId);

        // Выбираем нового Deputy если нужно
        if (context.getDeputyAddress() == null) {
            selectDeputy();
        }
    }

    /**
     * Обработка Discover сообщения
     */
    private void handleDiscover(SnakesProto.GameMessage message, InetSocketAddress sender) {
        // Отправляем Announcement в ответ
        announceGame();
    }

    private void sendError(String errorMessage, InetSocketAddress destination) {
        SnakesProto.GameMessage error = MessageBuilder.createError(errorMessage, 0);
        context.getNetworkManager().send(error, destination);
    }

    /**
     * Выбирает Deputy из активных игроков
     */
    private void selectDeputy() {
        // Логика выбора заместителя (например, игрок с наибольшим uptime)
        // Упрощенная версия - выбираем первого NORMAL игрока

        for (Player player : gameEngine.getGameState().getPlayers()) {
            if (player.getRole() == NodeRole.NORMAL &&
                    player.getId() != context.getLocalPlayer().getId()) {

                // Назначаем Deputy
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
}
