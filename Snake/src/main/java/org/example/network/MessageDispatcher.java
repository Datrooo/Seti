package org.example.network;

import org.example.protocol.SnakesProto;
import org.example.util.Logger;

import java.net.InetSocketAddress;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

public class MessageDispatcher {

    public interface Subscription extends AutoCloseable {
        @Override
        void close();
    }

    private final CopyOnWriteArrayList<BiConsumer<SnakesProto.GameMessage, InetSocketAddress>> pingHandlers;
    private final CopyOnWriteArrayList<BiConsumer<SnakesProto.GameMessage, InetSocketAddress>> steerHandlers;
    private final CopyOnWriteArrayList<BiConsumer<SnakesProto.GameMessage, InetSocketAddress>> ackHandlers;
    private final CopyOnWriteArrayList<BiConsumer<SnakesProto.GameMessage, InetSocketAddress>> stateHandlers;
    private final CopyOnWriteArrayList<BiConsumer<SnakesProto.GameMessage, InetSocketAddress>> announcementHandlers;
    private final CopyOnWriteArrayList<BiConsumer<SnakesProto.GameMessage, InetSocketAddress>> joinHandlers;
    private final CopyOnWriteArrayList<BiConsumer<SnakesProto.GameMessage, InetSocketAddress>> errorHandlers;
    private final CopyOnWriteArrayList<BiConsumer<SnakesProto.GameMessage, InetSocketAddress>> roleChangeHandlers;
    private final CopyOnWriteArrayList<BiConsumer<SnakesProto.GameMessage, InetSocketAddress>> discoverHandlers;

    public MessageDispatcher() {
        this.pingHandlers = new CopyOnWriteArrayList<>();
        this.steerHandlers = new CopyOnWriteArrayList<>();
        this.ackHandlers = new CopyOnWriteArrayList<>();
        this.stateHandlers = new CopyOnWriteArrayList<>();
        this.announcementHandlers = new CopyOnWriteArrayList<>();
        this.joinHandlers = new CopyOnWriteArrayList<>();
        this.errorHandlers = new CopyOnWriteArrayList<>();
        this.roleChangeHandlers = new CopyOnWriteArrayList<>();
        this.discoverHandlers = new CopyOnWriteArrayList<>();
    }

    public void dispatch(byte[] data, InetSocketAddress sender) {
        try {
            SnakesProto.GameMessage message = SnakesProto.GameMessage.parseFrom(data);
            
            String messageType = "UNKNOWN";
            if (message.hasPing()) messageType = "PING";
            else if (message.hasSteer()) messageType = "STEER";
            else if (message.hasAck()) messageType = "ACK";
            else if (message.hasState()) messageType = "STATE";
            else if (message.hasAnnouncement()) messageType = "ANNOUNCEMENT";
            else if (message.hasJoin()) messageType = "JOIN";
            else if (message.hasError()) messageType = "ERROR";
            else if (message.hasRoleChange()) messageType = "ROLE_CHANGE";
            else if (message.hasDiscover()) messageType = "DISCOVER";
            
            Logger.info("[DISPATCH] {} seq={} from {} | sender_id={} receiver_id={}", 
                    messageType, message.getMsgSeq(), sender,
                    message.hasSenderId() ? message.getSenderId() : "none",
                    message.hasReceiverId() ? message.getReceiverId() : "none");

            if (message.hasPing()) {
                notifyHandlers(pingHandlers, message, sender);
            } else if (message.hasSteer()) {
                notifyHandlers(steerHandlers, message, sender);
            } else if (message.hasAck()) {
                notifyHandlers(ackHandlers, message, sender);
            } else if (message.hasState()) {
                notifyHandlers(stateHandlers, message, sender);
            } else if (message.hasAnnouncement()) {
                notifyHandlers(announcementHandlers, message, sender);
            } else if (message.hasJoin()) {
                notifyHandlers(joinHandlers, message, sender);
            } else if (message.hasError()) {
                notifyHandlers(errorHandlers, message, sender);
            } else if (message.hasRoleChange()) {
                notifyHandlers(roleChangeHandlers, message, sender);
            } else if (message.hasDiscover()) {
                notifyHandlers(discoverHandlers, message, sender);
            } else {
                Logger.warn("Unknown message type from {}", sender);
            }
        } catch (Exception e) {
            Logger.error("Failed to parse message from {}: {}", sender, e.getMessage());
        }
    }

    private void notifyHandlers(
            CopyOnWriteArrayList<BiConsumer<SnakesProto.GameMessage, InetSocketAddress>> handlers,
            SnakesProto.GameMessage message,
            InetSocketAddress sender
    ) {
        for (BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler : handlers) {
            try {
                handler.accept(message, sender);
            } catch (Exception e) {
                Logger.error("Error in message handler: {}", e.getMessage(), e);
            }
        }
    }

    private Subscription subscribe(
            CopyOnWriteArrayList<BiConsumer<SnakesProto.GameMessage, InetSocketAddress>> list,
            BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler
    ) {
        list.add(handler);
        return () -> list.remove(handler);
    }

    public Subscription subscribePing(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        return subscribe(pingHandlers, handler);
    }

    public Subscription subscribeSteer(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        return subscribe(steerHandlers, handler);
    }

    public Subscription subscribeAck(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        return subscribe(ackHandlers, handler);
    }

    public Subscription subscribeState(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        return subscribe(stateHandlers, handler);
    }

    public Subscription subscribeJoin(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        return subscribe(joinHandlers, handler);
    }

    public Subscription subscribeError(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        return subscribe(errorHandlers, handler);
    }

    public Subscription subscribeRoleChange(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        return subscribe(roleChangeHandlers, handler);
    }

    public Subscription subscribeDiscover(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        return subscribe(discoverHandlers, handler);
    }

    public void onAck(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        ackHandlers.add(handler);
    }

}
