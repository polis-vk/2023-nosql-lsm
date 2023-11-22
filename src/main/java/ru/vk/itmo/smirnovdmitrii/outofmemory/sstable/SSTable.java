package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

/**
 * Representation of mapped SSTable.
 */
public class SSTable extends AbstractSSTable {
    private MemorySegment mapped;

    public SSTable(final MemorySegment mapped, final Path path, final long priority) {
        super(path, priority);
        this.mapped = mapped;
    }

    /**
     * After that you can't open SSTable for reading.
     */
    public void kill() {
        mapped = null;
    }

    /**
     * Opens SSTable for reading. Returns null if SSTable was deleted/compacted.
     * @return Opened representation of current SSTable.
     */
    public OpenedSSTable open() {
        final MemorySegment segment = mapped;
        if (segment == null) {
            return null;
        }
        return new OpenedSSTable(segment, path, priority);
    }

}
