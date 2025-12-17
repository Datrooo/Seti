package org.example.network;

import org.example.protocol.SnakesProto;
import org.example.util.Logger;

import java.net.InetSocketAddress;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

public class MessageDispatcher {
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

    public void resetAllHandlers() {
        pingHandlers.clear();
        steerHandlers.clear();
        ackHandlers.clear();
        stateHandlers.clear();
        announcementHandlers.clear();
        joinHandlers.clear();
        errorHandlers.clear();
        roleChangeHandlers.clear();
        discoverHandlers.clear();
    }


    /**
     * Обрабатывает входящее сообщение и направляет его соответствующим обработчикам
     */
    public void dispatch(byte[] data, InetSocketAddress sender) {
        try {
            SnakesProto.GameMessage message = SnakesProto.GameMessage.parseFrom(data);

            Logger.debug("Dispatching message seq={} from {}", message.getMsgSeq(), sender);

            // Определяем тип сообщения и вызываем соответствующие обработчики
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
            InetSocketAddress sender) {

        for (BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler : handlers) {
            try {
                handler.accept(message, sender);
            } catch (Exception e) {
                Logger.error("Error in message handler: {}", e.getMessage(), e);
            }
        }
    }

    // Методы для регистрации обработчиков

    public void onPing(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        pingHandlers.add(handler);
    }

    public void onSteer(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        steerHandlers.add(handler);
    }

    public void onAck(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        ackHandlers.add(handler);
    }

    public void onState(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        stateHandlers.add(handler);
    }

    public void onAnnouncement(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        announcementHandlers.add(handler);
    }

    public void onJoin(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        joinHandlers.add(handler);
    }

    public void onError(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        errorHandlers.add(handler);
    }

    public void onRoleChange(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        roleChangeHandlers.add(handler);
    }

    public void onDiscover(BiConsumer<SnakesProto.GameMessage, InetSocketAddress> handler) {
        discoverHandlers.add(handler);
    }
}
