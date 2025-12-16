package org.example.game.serialization;

import org.example.game.model.Direction;
import org.example.game.model.GameConfig;
import org.example.game.model.GameState;
import org.example.node.NodeRole;
import org.example.protocol.SnakesProto;
import org.example.util.IdGenerator;

import java.util.List;

public class MessageBuilder {

    /**
     * Создает PingMsg
     */
    public static SnakesProto.GameMessage createPing(int senderId, int receiverId) {
        return SnakesProto.GameMessage.newBuilder()
                .setMsgSeq(IdGenerator.generateMessageSeq())
                .setSenderId(senderId)
                .setReceiverId(receiverId)
                .setPing(SnakesProto.GameMessage.PingMsg.newBuilder().build())
                .build();
    }

    /**
     * Создает SteerMsg
     */
    public static SnakesProto.GameMessage createSteer(int senderId, Direction direction) {
        SnakesProto.Direction protoDir = switch (direction) {
            case UP -> SnakesProto.Direction.UP;
            case DOWN -> SnakesProto.Direction.DOWN;
            case LEFT -> SnakesProto.Direction.LEFT;
            case RIGHT -> SnakesProto.Direction.RIGHT;
        };

        return SnakesProto.GameMessage.newBuilder()
                .setMsgSeq(IdGenerator.generateMessageSeq())
                .setSenderId(senderId)
                .setSteer(SnakesProto.GameMessage.SteerMsg.newBuilder()
                        .setDirection(protoDir)
                        .build())
                .build();
    }

    /**
     * Создает AckMsg
     */
    public static SnakesProto.GameMessage createAck(long msgSeq, int senderId, int receiverId) {
        return SnakesProto.GameMessage.newBuilder()
                .setMsgSeq(msgSeq)
                .setSenderId(senderId)
                .setReceiverId(receiverId)
                .setAck(SnakesProto.GameMessage.AckMsg.newBuilder().build())
                .build();
    }

    /**
     * Создает StateMsg
     */
    public static SnakesProto.GameMessage createState(GameState state, int senderId) {
        SnakesProto.GameState protoState = StateSerializer.toProto(state);

        return SnakesProto.GameMessage.newBuilder()
                .setMsgSeq(IdGenerator.generateMessageSeq())
                .setSenderId(senderId)
                .setState(SnakesProto.GameMessage.StateMsg.newBuilder()
                        .setState(protoState)
                        .build())
                .build();
    }

    /**
     * Создает AnnouncementMsg
     */
    public static SnakesProto.GameMessage createAnnouncement(
            String gameName,
            GameConfig config,
            GameState state,
            boolean canJoin) {

        SnakesProto.GameAnnouncement announcement = SnakesProto.GameAnnouncement.newBuilder()
                .setGameName(gameName)
                .setConfig(StateSerializer.configToProto(config))
                .setPlayers(StateSerializer.toProto(state).getPlayers())
                .setCanJoin(canJoin)
                .build();

        return SnakesProto.GameMessage.newBuilder()
                .setMsgSeq(IdGenerator.generateMessageSeq())
                .setAnnouncement(SnakesProto.GameMessage.AnnouncementMsg.newBuilder()
                        .addGames(announcement)
                        .build())
                .build();
    }

    /**
     * Создает JoinMsg
     */
    public static SnakesProto.GameMessage createJoin(
            String playerName,
            String gameName,
            NodeRole requestedRole) {

        SnakesProto.NodeRole protoRole = switch (requestedRole) {
            case NORMAL -> SnakesProto.NodeRole.NORMAL;
            case VIEWER -> SnakesProto.NodeRole.VIEWER;
            default -> throw new IllegalArgumentException("Can only request NORMAL or VIEWER role");
        };

        return SnakesProto.GameMessage.newBuilder()
                .setMsgSeq(IdGenerator.generateMessageSeq())
                .setJoin(SnakesProto.GameMessage.JoinMsg.newBuilder()
                        .setPlayerName(playerName)
                        .setGameName(gameName)
                        .setRequestedRole(protoRole)
                        .setPlayerType(SnakesProto.PlayerType.HUMAN)
                        .build())
                .build();
    }

    /**
     * Создает ErrorMsg
     */
    public static SnakesProto.GameMessage createError(String errorMessage, int receiverId) {
        return SnakesProto.GameMessage.newBuilder()
                .setMsgSeq(IdGenerator.generateMessageSeq())
                .setReceiverId(receiverId)
                .setError(SnakesProto.GameMessage.ErrorMsg.newBuilder()
                        .setErrorMessage(errorMessage)
                        .build())
                .build();
    }

    /**
     * Создает RoleChangeMsg
     */
    public static SnakesProto.GameMessage createRoleChange(
            int senderId,
            int receiverId,
            NodeRole senderRole,
            NodeRole receiverRole) {

        SnakesProto.GameMessage.RoleChangeMsg.Builder builder =
                SnakesProto.GameMessage.RoleChangeMsg.newBuilder();

        if (senderRole != null) {
            builder.setSenderRole(nodeRoleToProto(senderRole));
        }

        if (receiverRole != null) {
            builder.setReceiverRole(nodeRoleToProto(receiverRole));
        }

        return SnakesProto.GameMessage.newBuilder()
                .setMsgSeq(IdGenerator.generateMessageSeq())
                .setSenderId(senderId)
                .setReceiverId(receiverId)
                .setRoleChange(builder.build())
                .build();
    }

    /**
     * Создает DiscoverMsg
     */
    public static SnakesProto.GameMessage createDiscover() {
        return SnakesProto.GameMessage.newBuilder()
                .setMsgSeq(IdGenerator.generateMessageSeq())
                .setDiscover(SnakesProto.GameMessage.DiscoverMsg.newBuilder().build())
                .build();
    }

    private static SnakesProto.NodeRole nodeRoleToProto(NodeRole role) {
        return switch (role) {
            case MASTER -> SnakesProto.NodeRole.MASTER;
            case DEPUTY -> SnakesProto.NodeRole.DEPUTY;
            case NORMAL -> SnakesProto.NodeRole.NORMAL;
            case VIEWER -> SnakesProto.NodeRole.VIEWER;
        };
    }
}
