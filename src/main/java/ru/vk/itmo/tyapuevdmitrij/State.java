package ru.vk.itmo.tyapuevdmitrij;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicLong;

public class State {
    final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable;
    final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> flushMemTable;
    final AtomicLong memoryUsage;
    final Storage storage;

    State(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable,
          ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> flushMemTable,
          Storage storage) {
        this.memTable = memTable;
        this.flushMemTable = flushMemTable;
        this.memoryUsage = new AtomicLong();
        this.storage = storage;
    }
}


