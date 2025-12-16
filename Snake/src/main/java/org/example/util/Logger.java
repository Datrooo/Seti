package org.example.util;

public class Logger {
    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger("SnakesGame");

    public static void info(String message, Object... args) {
        log.info(message, args);
    }

    public static void debug(String message, Object... args) {
        log.debug(message, args);
    }

    public static void error(String message, Object... args) {
        log.error(message, args);
    }

    public static void warn(String message, Object... args) {
        log.warn(message, args);
    }
}


