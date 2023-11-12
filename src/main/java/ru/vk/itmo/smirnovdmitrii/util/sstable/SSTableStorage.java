package ru.vk.itmo.smirnovdmitrii.util.sstable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface SSTableStorage extends Closeable, Iterable<SSTable> {
    /**
     * Thread save compaction method for storage. Replaces {@code compacted} SSTables with one {@code compaction}.
     * @param compaction representing compacted files.
     * @param compacted representing files that was compacted.
     */
    void compact(final SSTable compaction, final List<SSTable> compacted) throws IOException;

    /**
     * Adding SSTable to storage.
     * @param ssTable SSTable to add.
     */
    void add(final SSTable ssTable) throws IOException;

    @Override
    void close();
}
