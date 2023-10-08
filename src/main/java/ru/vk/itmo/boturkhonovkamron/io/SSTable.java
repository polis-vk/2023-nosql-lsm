package ru.vk.itmo.boturkhonovkamron.io;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
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
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Simple realization of Sorted Strings Table (SSTable) to store and read data from file.
 *
 * @author Kamron Boturkhonov
 * @since 2023.10.01
 */
public class SSTable {

    private static final String INDEX_FILE = "index.dat";

    private static final String TABLE_FILE = "ssTable.dat";

    private static final Set<StandardOpenOption> READ_OPTIONS = Set.of(READ);

    private static final Set<StandardOpenOption> READ_WRITE_OPTIONS = Set.of(READ, WRITE, CREATE, TRUNCATE_EXISTING);

    private final MemorySegment indexFileMap;

    private final MemorySegment tableFileMap;

    private final Arena indexReadArena;

    private final Arena tableReadArena;

    private final Path basePath;

    public SSTable(final Config config) throws IOException {
        this.basePath = config.basePath();
        final Path indexPath = basePath.resolve(INDEX_FILE);
        final Path tablePath = basePath.resolve(TABLE_FILE);
        this.indexReadArena = Arena.ofConfined();
        this.tableReadArena = Arena.ofConfined();
        if (Files.exists(indexPath) && Files.exists(tablePath)) {
            try (FileChannel indexChannel = FileChannel.open(indexPath, READ_OPTIONS);
                 FileChannel tableChannel = FileChannel.open(tablePath, READ_OPTIONS)) {
                indexFileMap = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexPath),
                        indexReadArena);
                tableFileMap = tableChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(tablePath),
                        tableReadArena);
            }
        } else {
            indexFileMap = null;
            tableFileMap = null;
        }
    }

    public Entry<MemorySegment> getEntity(final MemorySegment key) {
        if (tableFileMap == null || indexFileMap == null) {
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

    private long searchKeyPosition(final MemorySegment key) {
        final MemorySegmentComparator comparator = new MemorySegmentComparator();
        long low = 0;
        long high = indexFileMap.byteSize() / Long.BYTES - 1;

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

    private MemorySegment keyAt(final long index) {
        final long keySizePos = indexFileMap.get(ValueLayout.JAVA_LONG_UNALIGNED, index * Long.BYTES);
        final long keySize = tableFileMap.get(ValueLayout.JAVA_LONG_UNALIGNED, keySizePos);
        final long keyPos = keySizePos + Long.BYTES;

        return tableFileMap.asSlice(keyPos, keySize);
    }

    private MemorySegment valueAt(final long index) {
        final long keySizePos = indexFileMap.get(ValueLayout.JAVA_LONG_UNALIGNED, index * Long.BYTES);
        final long keySize = tableFileMap.get(ValueLayout.JAVA_LONG_UNALIGNED, keySizePos);

        final long valueSizePos = keySizePos + Long.BYTES + keySize;
        final long valueSize = tableFileMap.get(ValueLayout.JAVA_LONG_UNALIGNED, valueSizePos);
        final long valuePos = valueSizePos + Long.BYTES;

        return tableFileMap.asSlice(valuePos, valueSize);
    }

    public void saveData(final NavigableMap<MemorySegment, Entry<MemorySegment>> data)
            throws IOException {
        final Path indexPath = basePath.resolve(INDEX_FILE);
        final Path tablePath = basePath.resolve(TABLE_FILE);
        if (!indexReadArena.scope().isAlive() || !tableReadArena.scope().isAlive()) {
            return;
        }
        indexReadArena.close();
        tableReadArena.close();

        try (Arena indexArena = Arena.ofConfined();
             Arena tableArena = Arena.ofConfined();
             FileChannel indexChannel = FileChannel.open(indexPath, READ_WRITE_OPTIONS);
             FileChannel tableChannel = FileChannel.open(tablePath, READ_WRITE_OPTIONS)) {

            final long indexFileSize = (long) data.size() * Long.BYTES;
            final long tableFileSize = data.values().stream().mapToLong(entry -> {
                final long valueSize = entry.value() == null ? 0L : entry.value().byteSize();
                return Long.BYTES * 2 + valueSize + entry.key().byteSize();
            }).sum();

            final MemorySegment indexMap = indexChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, indexFileSize, indexArena);
            final MemorySegment tableMap = tableChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, tableFileSize, tableArena);

            final AtomicLong indexOffset = new AtomicLong(0);
            final AtomicLong tableOffset = new AtomicLong(0);
            data.forEach((key, entry) -> {
                final long keySize = entry.key().byteSize();
                final long valueSize = entry.value() == null ? 0L : entry.value().byteSize();

                // Saving current entry offset to index file
                indexMap.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset.getAndAdd(Long.BYTES), tableOffset.get());

                // Saving key size and key to table file
                tableMap.set(ValueLayout.JAVA_LONG_UNALIGNED, tableOffset.getAndAdd(Long.BYTES), keySize);
                MemorySegment.copy(entry.key(), 0, tableMap, tableOffset.getAndAdd(keySize), keySize);

                // Saving value size and value to table file
                tableMap.set(ValueLayout.JAVA_LONG_UNALIGNED, tableOffset.getAndAdd(Long.BYTES), valueSize);
                if (valueSize > 0) {
                    MemorySegment.copy(entry.value(), 0, tableMap, tableOffset.getAndAdd(valueSize), valueSize);
                }
            });

        }
    }

}
