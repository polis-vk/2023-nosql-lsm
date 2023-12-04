package ru.vk.itmo.reference;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;

/**
 * Reference implementation of {@link Dao}.
 *
 * @author incubos
 */
public class ReferenceDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Config config;
    private final Arena arena;
    private volatile TableSet tableSet;

    public ReferenceDao(final Config config) throws IOException {
        this.config = config;
        this.arena = Arena.ofShared();

        // First complete promotion of compacted SSTables
        SSTables.promote(
                config.basePath(),
                0,
                1);

        this.tableSet =
                TableSet.from(
                        SSTables.discover(
                                arena,
                                config.basePath()));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(
            final MemorySegment from,
            final MemorySegment to) {
        return new LiveFilteringIterator(tableSet.get(from, to));
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        return tableSet.get(key);
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        tableSet.upsert(entry);
    }

    @Override
    public synchronized void flush() throws IOException {
        final Iterator<Entry<MemorySegment>> iterator =
                tableSet.memTable.get(null, null);
        if (!iterator.hasNext()) {
            // Nothing to flush
            return;
        }

        // Write
        final int sequence = tableSet.nextSequence();
        SSTables.dump(
                config.basePath(),
                sequence,
                iterator);

        // Attach
        final SSTable flushed =
                SSTables.open(
                        arena,
                        config.basePath(),
                        sequence);
        tableSet = tableSet.flushed(flushed);
    }

    @Override
    public synchronized void compact() throws IOException {
        final TableSet tableSet = this.tableSet;
        if (tableSet.ssTables.size() < 2) {
            // Nothing to compact
            return;
        }

        // Compact to 0
        SSTables.dump(
                config.basePath(),
                0,
                new LiveFilteringIterator(
                        tableSet.allDiskEntries()));

        // Open 0
        final SSTable compacted =
                SSTables.open(
                        arena,
                        config.basePath(),
                        0);

        // Replace old SSTables with compacted one to
        // keep serving requests
        this.tableSet = tableSet.compacted(compacted);

        // Remove compacted SSTables starting from the oldest ones.
        // If we crash, 0 contains all the data, and
        // it will be promoted on reopen.
        for (final SSTable ssTable : tableSet.ssTables.reversed()) {
            SSTables.remove(
                    config.basePath(),
                    ssTable.sequence);
        }

        // Promote zero to one (possibly replacing)
        SSTables.promote(
                config.basePath(),
                0,
                1);
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            // Closed
            return;
        }

        flush();
        arena.close();
    }
}
