package ru.vk.itmo.smirnovdmitrii.util.sstable;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public record SSTable(
        MemorySegment mapped,
        Path path,
        AtomicLong readers,
        AtomicBoolean isAlive
) {
    public SSTable(final MemorySegment mapped, final Path path) {
        this(mapped, path, new AtomicLong(0), new AtomicBoolean(true));
    }

    public void open() {
        readers.incrementAndGet();
    }

    public void close() {
        readers.decrementAndGet();
    }
}
