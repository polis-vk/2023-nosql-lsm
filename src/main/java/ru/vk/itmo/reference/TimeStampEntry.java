package ru.vk.itmo.reference;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

import static java.lang.System.currentTimeMillis;

public class TimeStampEntry implements Entry<MemorySegment> {

    private final Entry<MemorySegment> entry;
    private final long timeStamp;

    // for upsetting
    public TimeStampEntry (Entry<MemorySegment> entry){
        this.entry = entry;
        this.timeStamp = currentTimeMillis();
    }

    // for reading
    public TimeStampEntry (Entry<MemorySegment> entry, long timeStamp){
        this.entry = entry;
        this.timeStamp = timeStamp;
    }

    @Override
    public MemorySegment key() {
        return entry.key();
    }

    @Override
    public MemorySegment value() {
        return entry.value();
    }

    public Entry<MemorySegment> getClearEntry() {
        return entry;
    }

    public long timeStamp() {
        return timeStamp;
    }
}
