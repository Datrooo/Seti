package org.example.socks5;

public final class SocksConstants {
    private SocksConstants() {}

    public static final byte SOCKS_VERSION = 0x05;

    // Methods
    public static final byte METHOD_NO_AUTH = 0x00;
    public static final byte METHOD_NO_ACCEPTABLE = (byte) 0xFF;

    // Commands
    public static final byte CMD_CONNECT = 0x01;

    // Address types
    public static final byte ATYP_IPV4 = 0x01;
    public static final byte ATYP_DOMAIN = 0x03;
    public static final byte ATYP_IPV6 = 0x04;

    // Reply codes
    public static final byte REP_SUCCESS = 0x00;
    public static final byte REP_GENERAL_FAILURE = 0x01;
    public static final byte REP_CONNECTION_NOT_ALLOWED = 0x02;
    public static final byte REP_NETWORK_UNREACHABLE = 0x03;
    public static final byte REP_HOST_UNREACHABLE = 0x04;
    public static final byte REP_CONNECTION_REFUSED = 0x05;
    public static final byte REP_TTL_EXPIRED = 0x06;
    public static final byte REP_COMMAND_NOT_SUPPORTED = 0x07;
    public static final byte REP_ADDR_TYPE_NOT_SUPPORTED = 0x08;
}
