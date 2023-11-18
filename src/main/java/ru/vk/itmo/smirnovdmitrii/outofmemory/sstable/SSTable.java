package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Representation of mapped SSTable.
 */
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

    /**
     * Makes SSTable not alive. After that {@link #tryOpen()} will return false.
     */
    public void kill() {
        isAlive.set(true);
    }

    /**
     * Tries to open SSTable. You can read only from opened SSTable.
     * @return true if succeed open.
     */
    public boolean tryOpen() {
        readers.incrementAndGet();
        if (!isAlive.get()) {
            readers.decrementAndGet();
            return false;
        }
        return true;
    }

    /**
     * Closes opened SSTable. Closing not opened SSTable can provide unexpected behavior.
     */
    @Override
    public void close() {
        readers.decrementAndGet();
    }

    /**
     * Compares two SSTables by priority.
     */
    @Override
    public int compareTo(final SSTable o) {
        return Long.compare(o.priority, this.priority);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(this.priority);
    }
}
