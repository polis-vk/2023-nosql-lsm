package ru.vk.itmo.reference;

import java.util.concurrent.atomic.AtomicLong;

public class LamportTimeStamp {
    private final AtomicLong currentTimeStamp = new AtomicLong(0);

    public long getCurrentTimeStamp() {
        return currentTimeStamp.get();
    }

    public void setCurrentTimeStamp(long initialTimeStamp) {
        currentTimeStamp.set(initialTimeStamp);
    }

    public long incrementAndGet() {
        return currentTimeStamp.incrementAndGet();
    }
}
