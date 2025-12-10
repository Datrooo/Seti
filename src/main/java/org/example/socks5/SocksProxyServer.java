package org.example.socks5;

import org.xbill.DNS.Message;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

import static org.example.socks5.SocksConstants.*;

// TODO: dns resolver from apache


public class SocksProxyServer {

    private static final int DNS_MAX_RETRIES = 5;
    private static final long DNS_RETRY_TIMEOUT_MS = 2000L;
    private static final int DNS_INITIAL_SEND_COUNT = 2;

    private final int listenPort;
    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final DatagramChannel dnsChannel;
    private final InetSocketAddress dnsServerAddress;
    private final DnsClient dnsClient = new DnsClient();
    private final DnsTransactionRegistry dnsRegistry = new DnsTransactionRegistry();

    private volatile boolean running = true;

    private enum ChannelRole {
        SERVER,
        CLIENT,
        REMOTE,
        DNS
    }

    private record ChannelContext(ChannelRole role, Session session) {}

    public SocksProxyServer(int listenPort) throws IOException {
        this.listenPort = listenPort;
        this.selector = Selector.open();

        this.dnsServerAddress = new InetSocketAddress("8.8.8.8", 53);

        this.serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress(listenPort));
        SelectionKey acceptKey = serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        acceptKey.attach(new ChannelContext(ChannelRole.SERVER, null));

        this.dnsChannel = DatagramChannel.open();
        dnsChannel.configureBlocking(false);
        dnsChannel.connect(dnsServerAddress);
        SelectionKey dnsKey = dnsChannel.register(selector, SelectionKey.OP_READ);
        dnsKey.attach(new ChannelContext(ChannelRole.DNS, null));

        System.out.println("SOCKS5 proxy listening on port " + listenPort);
    }

    public void run() throws IOException {
        try {
            while (running) {
                int ready = selector.select();
                if (!running) {
                    break;
                }
                if (ready == 0) {
                    checkDnsTimeouts();
                    continue;
                }

                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> it = keys.iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    ChannelContext ctx = (ChannelContext) key.attachment();
                    if (ctx == null) {
                        continue;
                    }

                    try {
                        switch (ctx.role) {
                            case SERVER -> {
                                if (key.isAcceptable()) {
                                    handleAccept(key);
                                }
                            }
                            case DNS -> {
                                if (key.isReadable()) {
                                    handleDnsRead();
                                }
                            }
                            case CLIENT -> handleClientKey(key, ctx.session);
                            case REMOTE -> handleRemoteKey(key, ctx.session);
                        }
                    } catch (IOException e) {
                        if (ctx.session != null) {
                            closeSession(ctx.session);
                        }
                    }
                }

                checkDnsTimeouts();
            }
        } catch (ClosedSelectorException ignored) {}
        finally {
            cleanup();
            System.out.println("SOCKS5 proxy on port " + listenPort + " shut down.");
        }
    }

    public void stop() {
        running = false;
        try {
            selector.wakeup();
        } catch (Exception ignored) {}
    }

    private void cleanup() {
        try {
            for (SelectionKey key : selector.keys()) {
                ChannelContext ctx = (ChannelContext) key.attachment();
                if (ctx != null && ctx.session != null) {
                    try {
                        closeSession(ctx.session);
                    } catch (Exception ignored) {}
                }
                try {
                    key.channel().close();
                } catch (IOException ignored) {}
            }
        } catch (ClosedSelectorException ignored) {}

        try {
            selector.close();
        } catch (IOException ignored) {}

        try {
            serverChannel.close();
        } catch (IOException ignored) {}

        try {
            dnsChannel.close();
        } catch (IOException ignored) {}
    }



    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
        SocketChannel client = ssc.accept();
        if (client == null) {
            return;
        }
        client.configureBlocking(false);
        Session session = new Session(client);
        SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);
        session.clientKey = clientKey;
        clientKey.attach(new ChannelContext(ChannelRole.CLIENT, session));

        System.out.println("New client connected from " + client.getRemoteAddress());
    }

    private void handleDnsRead() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(512);
        SocketAddress from;
        while ((from = dnsChannel.receive(buf)) != null) {
            buf.flip();
            byte[] data = new byte[buf.remaining()];
            buf.get(data);
            buf.clear();

            try {
                Message response = new Message(data);
                int txId = response.getHeader().getID() & 0xFFFF;

                Session session = dnsRegistry.remove(txId);
                if (session == null) {
                    continue;
                }

                session.dnsTransactionId = 0;
                session.dnsQuery = null;
                session.dnsRetries = 0;

                InetAddress addr = dnsClient.parseResponse(data, txId);
                if (addr == null) {
                    sendSocksReplyAndClose(session, REP_HOST_UNREACHABLE);
                    continue;
                }

                System.out.println("Resolved " + session.targetHost + " -> " + addr.getHostAddress());
                startConnectToRemote(session, new InetSocketAddress(addr, session.targetPort));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleClientKey(SelectionKey key, Session session) throws IOException {
        if (!key.isValid()) {
            return;
        }

        if (key.isReadable()) {
            switch (session.state) {
                case GREETING, METHOD_REPLY_SENT, REQUEST -> readClientHandshakeAndRequest(session);
                case RELAY -> readFromClientForRelay(session);
                default -> {
                }
            }
        }

        if (!key.isValid()) {
            return;
        }

        if (key.isWritable()) {
            if (session.pendingClientWrite != null) {
                writePendingToClient(session);
            }

            if (session.pendingClientWrite == null && session.state == SocksState.RELAY) {
                writeToClientFromRelay(session);
            }
        }
    }

    private void handleRemoteKey(SelectionKey key, Session session) throws IOException {
        if (!key.isValid()) {
            return;
        }

        if (key.isConnectable()) {
            finishRemoteConnect(session);
        }

        if (!key.isValid()) {
            return;
        }

        if (key.isReadable()) {
            if (session.state == SocksState.RELAY) {
                readFromRemoteForRelay(session);
            }
        }

        if (!key.isValid()) {
            return;
        }

        if (key.isWritable()) {
            if (session.state == SocksState.RELAY) {
                writeToRemoteFromRelay(session);
            }
        }
    }

    private void readClientHandshakeAndRequest(Session session) throws IOException {
        SocketChannel client = session.clientChannel;
        ByteBuffer buf = session.handshakeBuffer;

        int n = client.read(buf);
        if (n == -1) {
            closeSession(session);
            return;
        }
        if (n == 0) {
            return;
        }

        buf.flip();
        boolean progress = true;
        while (progress) {
            progress = false;
            switch (session.state) {
                case GREETING -> {
                    if (!processGreeting(session, buf)) {
                        buf.compact();
                        return;
                    }
                    progress = true;
                }
                case METHOD_REPLY_SENT -> {
                    buf.compact();
                    return;
                }
                case REQUEST -> {
                    if (!processRequest(session, buf)) {
                        buf.compact();
                        return;
                    }
                    progress = true;
                }
                default -> {
                    buf.compact();
                    return;
                }
            }
        }
        buf.compact();
    }

    private boolean processGreeting(Session session, ByteBuffer buf) throws IOException {
        if (buf.remaining() < 2) {
            return false;
        }
        buf.mark();
        byte ver = buf.get();
        byte nMethods = buf.get();
        if (ver != SOCKS_VERSION) {
            closeSession(session);
            return false;
        }
        if (buf.remaining() < (nMethods & 0xFF)) {
            buf.reset();
            return false;
        }
        boolean noAuth = false;
        for (int i = 0; i < (nMethods & 0xFF); i++) {
            byte m = buf.get();
            if (m == METHOD_NO_AUTH) {
                noAuth = true;
            }
        }
        byte[] resp = new byte[2];
        resp[0] = SOCKS_VERSION;
        resp[1] = noAuth ? METHOD_NO_AUTH : METHOD_NO_ACCEPTABLE;
        session.pendingClientWrite = ByteBuffer.wrap(resp);
        enableWrite(session.clientKey);

        if (!noAuth) {
            session.state = SocksState.CLOSING;
        } else {
            session.state = SocksState.METHOD_REPLY_SENT;
        }

        return true;
    }

    private boolean processRequest(Session session, ByteBuffer buf) throws IOException {
        // VER CMD RSV ATYP DST.ADDR DST.PORT
        if (buf.remaining() < 4) {
            return false;
        }
        buf.mark();
        byte ver = buf.get();
        byte cmd = buf.get();
        byte rsv = buf.get();
        byte atyp = buf.get();

        if (ver != SOCKS_VERSION) {
            closeSession(session);
            return false;
        }
        if (cmd != CMD_CONNECT) {
            sendSocksReplyAndClose(session, REP_COMMAND_NOT_SUPPORTED);
            return false;
        }

        session.targetAtyp = atyp;

        switch (atyp) {
            case ATYP_IPV4 -> {
                if (buf.remaining() < 4 + 2) {
                    buf.reset();
                    return false;
                }
                byte[] ipv4 = new byte[4];
                buf.get(ipv4);
                int port = ((buf.get() & 0xFF) << 8) | (buf.get() & 0xFF);
                session.targetIpv4 = ipv4;
                session.targetPort = port;
                session.targetHost = null;
                InetAddress addr = InetAddress.getByAddress(ipv4);
                startConnectToRemote(session, new InetSocketAddress(addr, port));
            }
            case ATYP_DOMAIN -> {
                if (buf.remaining() < 1) {
                    buf.reset();
                    return false;
                }
                int len = buf.get() & 0xFF;
                if (buf.remaining() < len + 2) {
                    buf.reset();
                    return false;
                }
                byte[] nameBytes = new byte[len];
                buf.get(nameBytes);
                String host = new String(nameBytes);
                int port = ((buf.get() & 0xFF) << 8) | (buf.get() & 0xFF);

                session.targetHost = host;
                session.targetPort = port;
                startDnsResolve(session);
            }
            case ATYP_IPV6 -> {
                sendSocksReplyAndClose(session, REP_ADDR_TYPE_NOT_SUPPORTED);
            }
            default -> {
                sendSocksReplyAndClose(session, REP_ADDR_TYPE_NOT_SUPPORTED);
            }
        }

        return true;
    }

    private void writePendingToClient(Session session) throws IOException {
        if (session.pendingClientWrite == null) {
            disableWriteIfNoNeed(session.clientKey);
            return;
        }
        SocketChannel client = session.clientChannel;
        ByteBuffer buf = session.pendingClientWrite;
        client.write(buf);
        if (!buf.hasRemaining()) {
            session.pendingClientWrite = null;
            disableWriteIfNoNeed(session.clientKey);
            if (session.state == SocksState.METHOD_REPLY_SENT) {
                session.state = SocksState.REQUEST;
            } else if (session.state == SocksState.CLOSING) {
                closeSession(session);
            }
        }
    }

    private void sendSocksReplyAndClose(Session session, byte rep) throws IOException {
        // VER REP RSV ATYP BND.ADDR BND.PORT
        ByteBuffer buf = ByteBuffer.allocate(10);
        buf.put(SOCKS_VERSION);
        buf.put(rep);
        buf.put((byte) 0x00); // RSV
        buf.put(ATYP_IPV4);
        buf.put(new byte[]{0, 0, 0, 0});
        buf.putShort((short) 0);
        buf.flip();
        session.pendingClientWrite = buf;
        session.state = SocksState.CLOSING;
        enableWrite(session.clientKey);
    }

    private void sendDnsQuery(Session session, int count) throws IOException {
        if (session.dnsQuery == null) return;

        int sentSuccessfully = 0;
        byte[] query = session.dnsQuery;

        for (int i = 0; i < count; i++) {
            ByteBuffer buf = ByteBuffer.wrap(query);
            int written = dnsChannel.write(buf);

            if (written == 0) {
                // we can return here when timeout ends
                break;
            }

            if (written != query.length) {
                // probably never
                throw new IOException(
                        "Unexpected partial UDP write: " + written + " of " + query.length);
            }

            sentSuccessfully++;
        }

        if (sentSuccessfully > 0) {
            session.dnsLastSendTime = System.currentTimeMillis();
            session.dnsRetries += sentSuccessfully;
        }
    }


    private void startDnsResolve(Session session) throws IOException {
        try {
            int txId = dnsRegistry.register(session);
            session.dnsTransactionId = txId;

            byte[] query = dnsClient.buildQuery(session.targetHost, txId);
            session.dnsQuery = query;
            session.dnsRetries = 0;

            sendDnsQuery(session, DNS_INITIAL_SEND_COUNT);

            session.state = SocksState.RESOLVING;
        } catch (Exception e) {
            sendSocksReplyAndClose(session, REP_HOST_UNREACHABLE);
        }
    }

    private void startConnectToRemote(Session session, InetSocketAddress remoteAddress) throws IOException {
        SocketChannel remote = SocketChannel.open();
        remote.configureBlocking(false);
        boolean connected = remote.connect(remoteAddress);
        session.remoteChannel = remote;
        SelectionKey key = remote.register(selector, connected ? SelectionKey.OP_READ : SelectionKey.OP_CONNECT);
        session.remoteKey = key;
        key.attach(new ChannelContext(ChannelRole.REMOTE, session));
        session.state = connected ? SocksState.RELAY : SocksState.CONNECTING;

        if (connected) {
            sendSuccessReply(session);
        }
    }

    private void finishRemoteConnect(Session session) throws IOException {
        SocketChannel remote = session.remoteChannel;
        if (remote == null) {
            closeSession(session);
            return;
        }
        if (remote.finishConnect()) {
            session.remoteKey.interestOps(SelectionKey.OP_READ);
            sendSuccessReply(session);
            session.state = SocksState.RELAY;
        } else {
            sendSocksReplyAndClose(session, REP_HOST_UNREACHABLE);
        }
    }

    private void sendSuccessReply(Session session) throws IOException {
        InetSocketAddress local = (InetSocketAddress) session.remoteChannel.getLocalAddress();
        byte[] addrBytes = local.getAddress().getAddress();
        if (addrBytes.length != 4) {
            addrBytes = new byte[]{0, 0, 0, 0};
        }
        int port = local.getPort();

        ByteBuffer buf = ByteBuffer.allocate(10);
        buf.put(SOCKS_VERSION);
        buf.put(REP_SUCCESS);
        buf.put((byte) 0x00);
        buf.put(ATYP_IPV4);
        buf.put(addrBytes);
        buf.putShort((short) port);
        buf.flip();

        session.pendingClientWrite = buf;
        enableWrite(session.clientKey);
    }

    private void readFromClientForRelay(Session session) throws IOException {
        if (session.clientToRemoteHasData) {
            disableRead(session.clientKey);
            return;
        }
        SocketChannel client = session.clientChannel;
        ByteBuffer buf = session.clientToRemote;
        buf.clear();
        int n = client.read(buf);
        if (n == -1) {
            session.clientClosedInput = true;
            shutdownRemoteOutputIfPossible(session);
            return;
        }
        if (n == 0) {
            return;
        }
        buf.flip();
        session.clientToRemoteHasData = true;
        enableWrite(session.remoteKey);
    }

    private void writeToRemoteFromRelay(Session session) throws IOException {
        if (!session.clientToRemoteHasData) {
            disableWriteIfNoNeed(session.remoteKey);
            return;
        }
        SocketChannel remote = session.remoteChannel;
        ByteBuffer buf = session.clientToRemote;
        remote.write(buf);
        if (!buf.hasRemaining()) {
            session.clientToRemoteHasData = false;
            buf.clear();
            enableRead(session.clientKey);
            disableWriteIfNoNeed(session.remoteKey);
            if (session.clientClosedInput) {
                shutdownRemoteOutputIfPossible(session);
            }
        }
    }

    private void readFromRemoteForRelay(Session session) throws IOException {
        if (session.remoteToClientHasData) {
            disableRead(session.remoteKey);
            return;
        }
        SocketChannel remote = session.remoteChannel;
        ByteBuffer buf = session.remoteToClient;
        buf.clear();
        int n = remote.read(buf);
        if (n == -1) {
            session.remoteClosedInput = true;
            shutdownClientOutputIfPossible(session);
            return;
        }
        if (n == 0) {
            return;
        }
        buf.flip();
        session.remoteToClientHasData = true;
        enableWrite(session.clientKey);
    }

    private void writeToClientFromRelay(Session session) throws IOException {
        if (!session.remoteToClientHasData) {
            disableWriteIfNoNeed(session.clientKey);
            return;
        }
        SocketChannel client = session.clientChannel;
        ByteBuffer buf = session.remoteToClient;
        client.write(buf);
        if (!buf.hasRemaining()) {
            session.remoteToClientHasData = false;
            buf.clear();
            enableRead(session.remoteKey);
            disableWriteIfNoNeed(session.clientKey);
            if (session.remoteClosedInput) {
                shutdownClientOutputIfPossible(session);
            }
        }
    }

    private void shutdownRemoteOutputIfPossible(Session session) throws IOException {
        if (session.remoteChannel != null && !session.remoteClosedOutput && !session.clientToRemoteHasData) {
            try {
                session.remoteChannel.shutdownOutput();
            } catch (Exception ignored) {}
            session.remoteClosedOutput = true;
            if (session.remoteClosedInput) {
                closeRemoteChannel(session);
            }
        }
    }

    private void shutdownClientOutputIfPossible(Session session) throws IOException {
        if (!session.clientClosedOutput && !session.remoteToClientHasData) {
            try {
                session.clientChannel.shutdownOutput();
            } catch (Exception ignored) {}
            session.clientClosedOutput = true;
            if (session.clientClosedInput) {
                closeClientChannel(session);
            }
        }
    }

    private void closeSession(Session session) {
        try {
            if (session.dnsTransactionId != 0) {
                dnsRegistry.remove(session.dnsTransactionId);
                session.dnsTransactionId = 0;
            }
            session.dnsQuery = null;
        } catch (Exception ignored) {}

        try {
            closeClientChannel(session);
        } catch (IOException ignored) {}
        try {
            closeRemoteChannel(session);
        } catch (IOException ignored) {}
        session.state = SocksState.CLOSING;
    }

    private void closeClientChannel(Session session) throws IOException {
        if (session.clientKey != null) {
            session.clientKey.cancel();
        }
        if (session.clientChannel != null && session.clientChannel.isOpen()) {
            session.clientChannel.close();
        }
        session.clientClosedInput = true;
        session.clientClosedOutput = true;
    }

    private void closeRemoteChannel(Session session) throws IOException {
        if (session.remoteKey != null) {
            session.remoteKey.cancel();
        }
        if (session.remoteChannel != null && session.remoteChannel.isOpen()) {
            session.remoteChannel.close();
        }
        session.remoteClosedInput = true;
        session.remoteClosedOutput = true;
    }

    private void checkDnsTimeouts() throws IOException {
        long now = System.currentTimeMillis();

        for (Session s : dnsRegistry.getSessions()) {
            if (s.state != SocksState.RESOLVING || s.dnsQuery == null) {
                continue;
            }

            long elapsed = now - s.dnsLastSendTime;
            if (elapsed < DNS_RETRY_TIMEOUT_MS) {
                continue;
            }

            if (s.dnsRetries >= DNS_MAX_RETRIES) {
                dnsRegistry.remove(s.dnsTransactionId);
                s.dnsTransactionId = 0;
                s.dnsQuery = null;

                sendSocksReplyAndClose(s, REP_HOST_UNREACHABLE);
            } else {
                sendDnsQuery(s, 1);
            }
        }
    }

    private void enableWrite(SelectionKey key) {
        if (key == null) return;
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
    }

    private void disableWriteIfNoNeed(SelectionKey key) {
        if (key == null) return;
        int ops = key.interestOps();
        if ((ops & SelectionKey.OP_WRITE) != 0) {
            key.interestOps(ops & ~SelectionKey.OP_WRITE);
        }
    }

    private void enableRead(SelectionKey key) {
        if (key == null) return;
        key.interestOps(key.interestOps() | SelectionKey.OP_READ);
    }

    private void disableRead(SelectionKey key) {
        if (key == null) return;
        key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
    }
}
