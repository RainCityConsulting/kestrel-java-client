package com.rcc.kestrel;

public class QueueStats {
    private String name;
    private long items;
    private long bytes;
    private long totalItems;
    private long logSize;
    private long expiredItems;
    private long memItems;
    private long memBytes;
    private long age;
    private long discarded;
    private long waiters;
    private int openTransactions;

    public QueueStats(
            String name,
            long items,
            long bytes,
            long totalItems,
            long logSize,
            long expiredItems,
            long memItems,
            long memBytes,
            long age,
            long discarded,
            long waiters,
            int openTransactions)
    {
        this.name = name;
        this.items = items;
        this.bytes = bytes;
        this.totalItems = totalItems;
        this.logSize = logSize;
        this.expiredItems = expiredItems;
        this.memItems = memItems;
        this.memBytes = memBytes;
        this.age = age;
        this.discarded = discarded;
        this.waiters = waiters;
        this.openTransactions = openTransactions;
    }

    public String getName() {
        return this.name;
    }

    public long getItems() {
        return this.items;
    }

    public long getBytes() {
        return this.bytes;
    }

    public long getTotalItems() {
        return this.totalItems;
    }

    public long getLogSize() {
        return this.logSize;
    }

    public long getExpiredItems() {
        return this.expiredItems;
    }

    public long getMemItems() {
        return this.memItems;
    }

    public long getMemBytes() {
        return this.memBytes;
    }

    public long getAge() {
        return this.age;
    }

    public long getDiscarded() {
        return this.discarded;
    }

    public long getWaiters() {
        return this.waiters;
    }

    public int getOpenTransactions() {
        return this.openTransactions;
    }
}
