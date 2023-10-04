package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class PersistentDao extends DaoImpl {

    private final Config config;
    private static final ValueLayout.OfLong META_LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final long ENTRY_META_SIZE = META_LAYOUT.byteSize() * 2;
    public static final String SSTABLE_NAME = "sstable";
    public static final long TABLE_SIZE_LIMIT = 1024 * 8; // TODO: От балды

    public PersistentDao(final Config config) throws IOException {
        this.config = config;
        readMap();
    }

    private Path getSSTablePath() {
        return this.config.basePath().resolve(SSTABLE_NAME);
    }

    private MemorySegment readMemorySegment(final MemorySegment readable, final long[] offset, final long size) {
        final MemorySegment result = readable.asSlice(offset[0], size);
        offset[0] += size;
        return result;
    }

    private Entry<MemorySegment> readEntry(final MemorySegment readable, final long[] offset, final boolean limited) {

        final long keySize = readable.get(META_LAYOUT, offset[0]);
        offset[0] += META_LAYOUT.byteSize();
        final long valueSize = readable.get(META_LAYOUT, offset[0] + keySize);
        // Без учета размеров объектов
        if (limited && TABLE_SIZE_LIMIT - keySize - valueSize < offset[0]) {
            return null;
        }
        final MemorySegment key   = readMemorySegment(readable, offset, keySize);
        offset[0] += META_LAYOUT.byteSize();
        final MemorySegment value = readMemorySegment(readable, offset, valueSize);
        return new BaseEntry<>(key, value);
    }

    private Entry<MemorySegment> readEntry(final MemorySegment readable, long[] offset) {
        return readEntry(readable, offset, false);
    }

    private void readMap() throws IOException {
        final Path path = getSSTablePath();
        if (Files.notExists(path)) {
            return;
        }

        try (final FileChannel readerChannel = FileChannel.open(
                getSSTablePath(),
                StandardOpenOption.READ)
        ) {

            long[] offset = new long[]{0};
            final MemorySegment memorySegment = readerChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    offset[0],
                    readerChannel.size(),
                    Arena.ofAuto()
            );

            while (offset[0] < memorySegment.byteSize()) {
                final Entry<MemorySegment> entry = readEntry(memorySegment, offset, true);
                if (entry == null) {
                    return;
                }
                upsert(entry);
            }
        }
    }

    private long getTotalMapSize() {
        long totalSize = ENTRY_META_SIZE * storage.size();
        for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : storage.entrySet()) {
            totalSize += entry.getKey().byteSize() + entry.getValue().value().byteSize();
        }
        return totalSize;
    }

    private static long putMemorySegment(
            final MemorySegment writable,
            long offset,
            final MemorySegment memorySegment
    ) {
        final long memorySegmentSize = memorySegment.byteSize();
        writable.set(META_LAYOUT, offset, memorySegmentSize);
        offset += META_LAYOUT.byteSize();
        MemorySegment.copy(memorySegment, 0, writable, offset, memorySegmentSize);
        return offset + memorySegmentSize;
    }

    private static long putEntry(
            final MemorySegment writable,
            long offset,
            Map.Entry<MemorySegment, Entry<MemorySegment>> entry
    ) {
        offset = putMemorySegment(writable, offset, entry.getKey());
        return putMemorySegment(writable, offset, entry.getValue().value());
    }

    @Override
    public void close() throws IOException {
        final Path path = this.config.basePath().resolve(SSTABLE_NAME);
        try (Arena arena = Arena.ofConfined(); FileChannel writerChannel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
//                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)
        ) {
            long offset = 0;
            final MemorySegment memorySegment = writerChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    offset,
                    getTotalMapSize(),
                    arena
            );
            for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : storage.entrySet()) {
                offset = putEntry(memorySegment, offset, entry);
            }
        }
    }

    private Entry<MemorySegment> mapEntry(
            final Path path,
            final long offset,
            final long keySize,
            final long valueSize
    ) throws IOException {

        try (final FileChannel readerChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            final Arena arena = Arena.ofAuto();
            return new BaseEntry<>(
                    readerChannel.map(
                            FileChannel.MapMode.READ_ONLY,
                            offset,
                            keySize,
                            arena
                    ),
                    readerChannel.map(
                            FileChannel.MapMode.READ_ONLY,
                            offset + keySize + META_LAYOUT.byteSize(),
                            valueSize,
                            arena
                    )
            );
        }
    }

    private Entry<MemorySegment> getFromSSTable(final Path tablePath, final MemorySegment key) throws IOException {
        if (Files.notExists(tablePath)) {
            return null;
        }

        try (
                final Arena arena = Arena.ofConfined();
                final FileChannel readerChannel = FileChannel.open(
                        tablePath,
                        StandardOpenOption.READ
                )
        ) {
            long[] offset = new long[]{0};
            final MemorySegment memorySegment = readerChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    offset[0],
                    readerChannel.size(),
                    arena
            );

            while (offset[0] < memorySegment.byteSize()) {
                final Entry<MemorySegment> entry = readEntry(memorySegment, offset);
                if (COMPARATOR.compare(key, entry.key()) == 0) {
                    return mapEntry(
                            tablePath,
                            memorySegment.segmentOffset(entry.key()),
                            entry.key().byteSize(),
                            entry.value().byteSize()
                    );
                }
            }
        }
        return null;
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        final Entry<MemorySegment> result = super.get(key);
        if (result != null) {
            return result;
        }
        try {
            return getFromSSTable(getSSTablePath(), key);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            return null;
        }
    }
}
