package org.example.game.serialization;

import org.example.game.model.*;
import org.example.node.NodeRole;
import org.example.protocol.SnakesProto;

import java.net.InetSocketAddress;
import java.util.List;

public class StateSerializer {
    public static SnakesProto.GameState toProto(GameState state) { // Конвертирует GameState в Protobuf StateMsg
        SnakesProto.GameState.Builder builder = SnakesProto.GameState.newBuilder()
                .setStateOrder(state.getStateOrder())
                .setPlayers(playersToProto(state.getPlayers()));

        for (Snake snake : state.getSnakes()) {
            builder.addSnakes(snakeToProto(snake));
        }

        for (Coord food : state.getFoods()) {
            builder.addFoods(coordToProto(food));
        }
        return builder.build();
    }

    public static GameState fromProto(SnakesProto.GameState protoState, GameConfig config) { // Конвертирует Protobuf GameState в GameState
        GameState gameState = new GameState(config);

        for (SnakesProto.GamePlayer protoPlayer : protoState.getPlayers().getPlayersList()) {
            Player player = playerFromProto(protoPlayer);
            gameState.addPlayer(player);
        }

        for (SnakesProto.GameState.Snake protoSnake : protoState.getSnakesList()) {
            Snake snake = snakeFromProto(protoSnake);
            gameState.addSnake(snake);
        }

        for (SnakesProto.GameState.Coord protoFood : protoState.getFoodsList()) {
            Coord food = coordFromProto(protoFood);
            gameState.addFood(food);
        }

        gameState.setStateOrder(protoState.getStateOrder());

        return gameState;
    }

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

    private static SnakesProto.GameState.Snake snakeToProto(Snake snake) {
        SnakesProto.GameState.Snake.Builder builder = SnakesProto.GameState.Snake.newBuilder()
                .setPlayerId(snake.getPlayerId())
                .setHeadDirection(directionToProto(snake.getHeadDirection()))
                .setState(snakeStateToProto(snake.getState()));

        List<Coord> body = snake.getBody();
        if (!body.isEmpty()) {
            builder.addPoints(coordToProtoAbsolute(body.get(0)));

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
        Coord head = coordFromProto(protoSnake.getPoints(0));
        Snake snake = new Snake(
                protoSnake.getPlayerId(),
                head,
                directionFromProto(protoSnake.getHeadDirection())
        );
        Coord current = head;
        for (int i = 1; i < protoSnake.getPointsCount(); i++) {
            SnakesProto.GameState.Coord protoPoint = protoSnake.getPoints(i);
            int dx = protoPoint.getX();
            int dy = protoPoint.getY();
            current = new Coord(current.x() + dx, current.y() + dy);
            snake.getBodyInternal().add(current);
        }
        if (protoSnake.getState() == SnakesProto.GameState.Snake.SnakeState.ZOMBIE) {
            snake.setAlive(false);
        }
        return snake;
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
