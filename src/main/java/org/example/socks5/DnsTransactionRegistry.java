package org.example.socks5;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DnsTransactionRegistry {

    private final Map<Integer, Session> byId = new HashMap<>();
    private int nextId = 1;

    private final int MAX_COUNT_PORT = 65535;

    public synchronized int register(Session session) {
        for (int i = 0; i < MAX_COUNT_PORT; i++) {
            int id = nextId & 0xFFFF;
            nextId = (nextId + 1) & 0xFFFF;
            if (id == 0) continue;
            if (!byId.containsKey(id)) {
                byId.put(id, session);
                return id;
            }
        }
        throw new IllegalStateException("No free DNS transaction IDs");
    }

    public synchronized Session remove(int id) {
        return byId.remove(id & 0xFFFF);
    }

    public synchronized Collection<Session> getSessions() {
        return new ArrayList<>(byId.values());
    }

}
