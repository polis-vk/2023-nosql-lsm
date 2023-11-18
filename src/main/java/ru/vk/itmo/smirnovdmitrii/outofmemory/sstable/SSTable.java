package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

import java.io.Closeable;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public record SSTable(
        MemorySegment mapped,
        Path path,
        long priority,
        AtomicLong readers,
        AtomicBoolean isAlive
) implements Comparable<SSTable>, AutoCloseable {
    public SSTable(final MemorySegment mapped, final Path path, final long priority) {
        this(mapped, path, priority, new AtomicLong(0), new AtomicBoolean(true));
    }

    public void kill() {
        isAlive.set(true);
    }

    public boolean tryOpen() {
        readers.incrementAndGet();
        if (!isAlive.get()) {
            readers.decrementAndGet();
            return false;
        }
        return true;
    }

    @Override
    public void close() {
        readers.decrementAndGet();
    }

    @Override
    public int compareTo(final SSTable o) {
        final int compareResult = Long.compare(o.priority, this.priority);
        if (compareResult != 0) {
            return compareResult;
        }
        return path.compareTo(o.path);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(this.priority);
    }
}
