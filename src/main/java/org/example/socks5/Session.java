package org.example.socks5;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class Session {

    final SocketChannel clientChannel;
    SocketChannel remoteChannel;

    SelectionKey clientKey;
    SelectionKey remoteKey;

    SocksState state = SocksState.GREETING;

    public int dnsTransactionId = -1;
    public byte[] dnsQuery;
    public long dnsLastSendTime;
    public int dnsRetries;

    final ByteBuffer handshakeBuffer = ByteBuffer.allocate(1024);
    ByteBuffer pendingClientWrite;

    final ByteBuffer clientToRemote = ByteBuffer.allocateDirect(64 * 1024);
    final ByteBuffer remoteToClient = ByteBuffer.allocateDirect(64 * 1024);

    boolean clientToRemoteHasData = false;
    boolean remoteToClientHasData = false;

    boolean clientClosedInput = false;
    boolean clientClosedOutput = false;
    boolean remoteClosedInput = false;
    boolean remoteClosedOutput = false;

    String targetHost;
    int targetPort;
    byte targetAtyp;
    byte[] targetIpv4;

    public Session(SocketChannel clientChannel) {
        this.clientChannel = clientChannel;
        clientToRemote.clear();
        remoteToClient.clear();
    }

    @Override
    public String toString() {
        return "Session{" +
                "state=" + state +
                ", targetHost='" + targetHost + '\'' +
                ", targetPort=" + targetPort +
                '}';
    }
}
