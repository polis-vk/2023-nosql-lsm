package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface SSTableStorage extends Closeable, Iterable<SSTable> {
    /**
     * Thread save compaction method for storage. Replaces {@code compacted} SSTables with one {@code compaction}.
     * @param compactionFileName file with compacted data from SSTables.
     * @param compacted representing files that was compacted.
     */
    void compact(final String compactionFileName, final List<SSTable> compacted) throws IOException;

    /**
     * Adding SSTable to storage.
     * @param ssTableFileName SSTable to add.
     */
    void add(final String ssTableFileName) throws IOException;

    SSTable getLast();

    @Override
    void close();
}
