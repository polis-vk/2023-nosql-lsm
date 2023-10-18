package ru.vk.itmo.boturkhonovkamron.persistence;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.boturkhonovkamron.MemorySegmentComparator;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Set;

import static java.nio.file.StandardOpenOption.READ;

/**
 * Simple realization of Sorted Strings Table (SSTable) to store and read data from file.
 *
 * @author Kamron Boturkhonov
 * @since 2023.10.01
 */
public class SSTable {

    public static final String INDEX_FILE = "index.dat";

    public static final String TABLE_FILE = "ssTable.dat";

    private static final Set<StandardOpenOption> READ_OPTIONS = Set.of(READ);

    private final SSTableFileMap readFileMap;

    private final SSTableArena readArena;

    /**
     * Elements count in current table.
     */
    private final long size;

    public SSTable(final Path basePath, final Long version) throws IOException {
        final Path indexPath = basePath.resolve(version.toString()).resolve(INDEX_FILE);
        final Path tablePath = basePath.resolve(version.toString()).resolve(TABLE_FILE);
        this.readArena = new SSTableArena(Arena.ofConfined(), Arena.ofConfined());
        try (FileChannel indexChannel = FileChannel.open(indexPath, READ_OPTIONS);
                FileChannel tableChannel = FileChannel.open(tablePath, READ_OPTIONS)) {
            final MemorySegment indexMap =
                    indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexPath), readArena.indexArena());
            final MemorySegment tableMap =
                    tableChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(tablePath), readArena.tableArena());
            this.readFileMap = new SSTableFileMap(indexMap, tableMap);
            this.size = indexMap.byteSize() / Long.BYTES;
        }
    }

    public Entry<MemorySegment> getEntity(final MemorySegment key) {
        if (readFileMap.tableMap() == null || readFileMap.indexMap() == null) {
            return null;
        }
        final long keyPosition = searchKeyPosition(key);
        if (keyPosition < 0) {
            return null;
        }
        return entityAt(keyPosition);
    }

    private Entry<MemorySegment> entityAt(final long index) {
        return new BaseEntry<>(keyAt(index), valueAt(index));
    }

    public long searchKeyPosition(final MemorySegment key) {
        final Comparator<MemorySegment> comparator = MemorySegmentComparator.COMPARATOR;
        long low = 0;
        long high = this.size - 1;

        while (low <= high) {
            final long mid = (low + high) >>> 1;
            final MemorySegment midKey = keyAt(mid);
            final int cmp = comparator.compare(midKey, key);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }

        return -(low + 1);
    }

    public MemorySegment keyAt(final long index) {
        final long keySizePos = readFileMap.indexMap().get(ValueLayout.JAVA_LONG_UNALIGNED, index * Long.BYTES);
        final long keySize = readFileMap.tableMap().get(ValueLayout.JAVA_LONG_UNALIGNED, keySizePos);
        final long keyPos = keySizePos + Long.BYTES;

        return readFileMap.tableMap().asSlice(keyPos, keySize);
    }

    public MemorySegment valueAt(final long index) {
        final long keySizePos = readFileMap.indexMap().get(ValueLayout.JAVA_LONG_UNALIGNED, index * Long.BYTES);
        final long keySize = readFileMap.tableMap().get(ValueLayout.JAVA_LONG_UNALIGNED, keySizePos);

        final long valueSizePos = keySizePos + Long.BYTES + keySize;
        final long valueSize = readFileMap.tableMap().get(ValueLayout.JAVA_LONG_UNALIGNED, valueSizePos);
        final long valuePos = valueSizePos + Long.BYTES;

        return valueSize == 0 ? null : readFileMap.tableMap().asSlice(valuePos, valueSize);
    }

    public long size() {
        return size;
    }

    public boolean close() {
        if (this.readArena.isAlive()) {
            this.readArena.close();
            return true;
        }
        return false;
    }

}
