package org.example.game.model;

import org.example.node.NodeRole;

import java.net.InetSocketAddress;

public class Player {
    private int id;
    private final String name;
    private final InetSocketAddress address;
    private NodeRole role;
    private final PlayerType type;
    private int score;
    private long lastActivityTime;

    public Player(int id, String name, InetSocketAddress address,
                  NodeRole role, PlayerType type) {
        this.id = id;
        this.name = name;
        this.address = address;
        this.role = role;
        this.type = type;
        this.score = 0;
        this.lastActivityTime = System.currentTimeMillis();
    }

    public void incrementScore() {
        score++;
    }

    public void updateActivity() {
        this.lastActivityTime = System.currentTimeMillis();
    }

    public boolean isTimedOut(int timeoutMs) {
        return System.currentTimeMillis() - lastActivityTime > timeoutMs;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public NodeRole getRole() {
        return role;
    }

    public void setRole(NodeRole role) {
        this.role = role;
    }

    public PlayerType getType() {
        return type;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public long getLastActivityTime() {
        return lastActivityTime;
    }

    @Override
    public String toString() {
        return String.format("%s [%s] - %d pts", name, role, score);
    }

    public void setId(int id) {
        this.id = id;
    }
}
