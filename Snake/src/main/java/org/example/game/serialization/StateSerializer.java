package org.example.game.serialization;

import org.example.game.model.*;
import org.example.node.NodeRole;
import org.example.protocol.SnakesProto;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

public class StateSerializer {

    /**
     * Конвертирует GameState в Protobuf StateMsg
     */
    public static SnakesProto.GameState toProto(GameState state) {
        SnakesProto.GameState.Builder builder = SnakesProto.GameState.newBuilder()
                .setStateOrder(state.getStateOrder())
                .setPlayers(playersToProto(state.getPlayers()));

        // Конвертируем змеек
        for (Snake snake : state.getSnakes()) {
            builder.addSnakes(snakeToProto(snake));
        }

        // Конвертируем еду
        for (Coord food : state.getFoods()) {
            builder.addFoods(coordToProto(food));
        }

        return builder.build();
    }

    /**
     * Конвертирует Protobuf GameState в доменную модель
     */
    public static GameState fromProto(SnakesProto.GameState protoState, GameConfig config) {
        GameState state = new GameState(config);

        // Восстанавливаем игроков
        if (protoState.hasPlayers()) {
            for (SnakesProto.GamePlayer protoPlayer : protoState.getPlayers().getPlayersList()) {
                Player player = playerFromProto(protoPlayer);
                state.addPlayer(player);
            }
        }

        // Восстанавливаем змеек
        for (SnakesProto.GameState.Snake protoSnake : protoState.getSnakesList()) {
            Snake snake = snakeFromProto(protoSnake);
            state.addSnake(snake);
        }

        // Восстанавливаем еду
        for (SnakesProto.GameState.Coord protoCoord : protoState.getFoodsList()) {
            state.addFood(coordFromProto(protoCoord));
        }

        return state;
    }

    /**
     * Конвертирует GameConfig в Protobuf
     */
    public static SnakesProto.GameConfig configToProto(GameConfig config) {
        return SnakesProto.GameConfig.newBuilder()
                .setWidth(config.width())
                .setHeight(config.height())
                .setFoodStatic(config.foodStatic())
                .setFoodPerPlayer(config.foodPerPlayer())
                .setStateDelayMs(config.stateDelayMs())
                .setDeadFoodProb(config.deadFoodProb())
                .setPingDelayMs(config.pingDelayMs())
                .setNodeTimeoutMs(config.nodeTimeoutMs())
                .build();
    }

    /**
     * Конвертирует Protobuf GameConfig в доменную модель
     */
    public static GameConfig configFromProto(SnakesProto.GameConfig protoConfig) {
        return new GameConfig(
                protoConfig.hasWidth() ? protoConfig.getWidth() : 40,
                protoConfig.hasHeight() ? protoConfig.getHeight() : 30,
                protoConfig.hasFoodStatic() ? protoConfig.getFoodStatic() : 1,
                protoConfig.hasFoodPerPlayer() ? protoConfig.getFoodPerPlayer() : 1,
                protoConfig.hasStateDelayMs() ? protoConfig.getStateDelayMs() : 1000,
                protoConfig.hasDeadFoodProb() ? protoConfig.getDeadFoodProb() : 0.1f,
                protoConfig.hasPingDelayMs() ? protoConfig.getPingDelayMs() : 1000,
                protoConfig.hasNodeTimeoutMs() ? protoConfig.getNodeTimeoutMs() : 3000
        );
    }

    // ========== Вспомогательные методы ==========

    private static SnakesProto.GameState.Snake snakeToProto(Snake snake) {
        SnakesProto.GameState.Snake.Builder builder = SnakesProto.GameState.Snake.newBuilder()
                .setPlayerId(snake.getPlayerId())
                .setHeadDirection(directionToProto(snake.getHeadDirection()))
                .setState(snakeStateToProto(snake.getState()));

        // Конвертируем координаты в относительные смещения
        List<Coord> body = snake.getBody();
        if (!body.isEmpty()) {
            // Первая точка - абсолютные координаты головы
            builder.addPoints(coordToProtoAbsolute(body.get(0)));

            // Остальные точки - относительные смещения
            for (int i = 1; i < body.size(); i++) {
                Coord current = body.get(i);
                Coord previous = body.get(i - 1);
                int dx = current.x() - previous.x();
                int dy = current.y() - previous.y();
                builder.addPoints(coordToProtoRelative(dx, dy));
            }
        }

        return builder.build();
    }

    private static Snake snakeFromProto(SnakesProto.GameState.Snake protoSnake) {
        List<SnakesProto.GameState.Coord> protoPoints = protoSnake.getPointsList();

        if (protoPoints.isEmpty()) {
            throw new IllegalArgumentException("Snake must have at least one point");
        }

        // Восстанавливаем абсолютные координаты из относительных смещений
        List<Coord> absoluteCoords = new java.util.ArrayList<>();

        // Первая точка - абсолютные координаты головы
        int currentX = protoPoints.get(0).getX();
        int currentY = protoPoints.get(0).getY();
        absoluteCoords.add(new Coord(currentX, currentY));

        // Остальные точки - применяем смещения
        for (int i = 1; i < protoPoints.size(); i++) {
            currentX += protoPoints.get(i).getX();
            currentY += protoPoints.get(i).getY();
            absoluteCoords.add(new Coord(currentX, currentY));
        }

        Direction direction = directionFromProto(protoSnake.getHeadDirection());
        Snake.SnakeState state = protoSnake.getState() == SnakesProto.GameState.Snake.SnakeState.ZOMBIE
                ? Snake.SnakeState.ZOMBIE
                : Snake.SnakeState.ALIVE;

        // Используем новый конструктор
        return new Snake(protoSnake.getPlayerId(), absoluteCoords, direction, state);
    }


    private static SnakesProto.GamePlayers playersToProto(Iterable<Player> players) {
        SnakesProto.GamePlayers.Builder builder = SnakesProto.GamePlayers.newBuilder();

        for (Player player : players) {
            builder.addPlayers(playerToProto(player));
        }

        return builder.build();
    }

    private static SnakesProto.GamePlayer playerToProto(Player player) {
        SnakesProto.GamePlayer.Builder builder = SnakesProto.GamePlayer.newBuilder()
                .setId(player.getId())
                .setName(player.getName())
                .setRole(nodeRoleToProto(player.getRole()))
                .setScore(player.getScore());

        // Optional поля
        if (player.getAddress() != null) {
            builder.setIpAddress(player.getAddress().getAddress().getHostAddress());
            builder.setPort(player.getAddress().getPort());
        }

        builder.setType(playerTypeToProto(player.getType()));

        return builder.build();
    }

    private static Player playerFromProto(SnakesProto.GamePlayer protoPlayer) {
        InetSocketAddress address = null;

        if (protoPlayer.hasIpAddress() && protoPlayer.hasPort()) {
            address = new InetSocketAddress(
                    protoPlayer.getIpAddress(),
                    protoPlayer.getPort()
            );
        }

        PlayerType type = protoPlayer.hasType()
                ? playerTypeFromProto(protoPlayer.getType())
                : PlayerType.HUMAN;

        Player player = new Player(
                protoPlayer.getId(),
                protoPlayer.getName(),
                address,
                nodeRoleFromProto(protoPlayer.getRole()),
                type
        );

        player.setScore(protoPlayer.getScore());
        return player;
    }

    private static SnakesProto.GameState.Coord coordToProto(Coord coord) {
        return coordToProtoAbsolute(coord);
    }

    private static SnakesProto.GameState.Coord coordToProtoAbsolute(Coord coord) {
        return SnakesProto.GameState.Coord.newBuilder()
                .setX(coord.x())
                .setY(coord.y())
                .build();
    }

    private static SnakesProto.GameState.Coord coordToProtoRelative(int dx, int dy) {
        return SnakesProto.GameState.Coord.newBuilder()
                .setX(dx)
                .setY(dy)
                .build();
    }

    private static Coord coordFromProto(SnakesProto.GameState.Coord protoCoord) {
        int x = protoCoord.hasX() ? protoCoord.getX() : 0;
        int y = protoCoord.hasY() ? protoCoord.getY() : 0;
        return new Coord(x, y);
    }

    private static SnakesProto.Direction directionToProto(Direction direction) {
        return switch (direction) {
            case UP -> SnakesProto.Direction.UP;
            case DOWN -> SnakesProto.Direction.DOWN;
            case LEFT -> SnakesProto.Direction.LEFT;
            case RIGHT -> SnakesProto.Direction.RIGHT;
        };
    }

    public static Direction directionFromProto(SnakesProto.Direction protoDir) {
        return switch (protoDir) {
            case UP -> Direction.UP;
            case DOWN -> Direction.DOWN;
            case LEFT -> Direction.LEFT;
            case RIGHT -> Direction.RIGHT;
            default -> throw new IllegalArgumentException("Unknown direction: " + protoDir);
        };
    }

    public static SnakesProto.NodeRole nodeRoleToProto(NodeRole role) {
        return switch (role) {
            case MASTER -> SnakesProto.NodeRole.MASTER;
            case DEPUTY -> SnakesProto.NodeRole.DEPUTY;
            case NORMAL -> SnakesProto.NodeRole.NORMAL;
            case VIEWER -> SnakesProto.NodeRole.VIEWER;
        };
    }

    public static NodeRole nodeRoleFromProto(SnakesProto.NodeRole protoRole) {
        return switch (protoRole) {
            case MASTER -> NodeRole.MASTER;
            case DEPUTY -> NodeRole.DEPUTY;
            case NORMAL -> NodeRole.NORMAL;
            case VIEWER -> NodeRole.VIEWER;
            default -> throw new IllegalArgumentException("Unknown role: " + protoRole);
        };
    }

    private static SnakesProto.PlayerType playerTypeToProto(PlayerType type) {
        return switch (type) {
            case HUMAN -> SnakesProto.PlayerType.HUMAN;
            case ROBOT -> SnakesProto.PlayerType.ROBOT;
        };
    }

    public static PlayerType playerTypeFromProto(SnakesProto.PlayerType protoType) {
        return switch (protoType) {
            case HUMAN -> PlayerType.HUMAN;
            case ROBOT -> PlayerType.ROBOT;
            default -> throw new IllegalArgumentException("Unknown player type: " + protoType);
        };
    }

    private static SnakesProto.GameState.Snake.SnakeState snakeStateToProto(Snake.SnakeState state) {
        return switch (state) {
            case ALIVE -> SnakesProto.GameState.Snake.SnakeState.ALIVE;
            case ZOMBIE -> SnakesProto.GameState.Snake.SnakeState.ZOMBIE;
        };
    }
}
