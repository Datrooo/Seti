package org.example.socks5;

public enum SocksState {
    GREETING,
    METHOD_REPLY_SENT,
    REQUEST,
    RESOLVING,
    CONNECTING,
    RELAY,
    CLOSING
}
// todo: sealed classes