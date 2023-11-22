package ru.vk.itmo.kononovvladimir;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicLong;

public class State {

    final NavigableMap<MemorySegment, Entry<MemorySegment>> memoryStorage;
    final NavigableMap<MemorySegment, Entry<MemorySegment>> flushingMemoryTable;
    final AtomicLong memoryStorageSizeInBytes = new AtomicLong();
    final DiskStorage diskStorage;

    public State(NavigableMap<MemorySegment, Entry<MemorySegment>> memoryStorage,
                 NavigableMap<MemorySegment, Entry<MemorySegment>> flushingMemoryTable,
                 DiskStorage diskStorage) {
        this.memoryStorage = memoryStorage;
        this.flushingMemoryTable = flushingMemoryTable;
        this.memoryStorageSizeInBytes.getAndSet(memoryStorage.size());
        this.diskStorage = diskStorage;
    }
}