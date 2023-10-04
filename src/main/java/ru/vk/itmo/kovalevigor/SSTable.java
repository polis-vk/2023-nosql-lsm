package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class SSTable {

    private final Path path;
    private static final ValueLayout.OfLong META_LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final long ENTRY_META_SIZE = META_LAYOUT.byteSize() * 2;
    public static final Comparator<MemorySegment> COMPARATOR = UtilsMemorySegment::compare;

    public SSTable(final Path path) {
        this.path = path;
    }

    private MemorySegment readMemorySegment(final MemorySegment readable, final long[] offset, final long size) {
        final MemorySegment result = readable.asSlice(offset[0], size);
        offset[0] += size;
        return result;
    }

    private Entry<MemorySegment> readEntry(final MemorySegment readable, final long[] offset, final long limited) {

        final long keySize = readable.get(META_LAYOUT, offset[0]);
        offset[0] += META_LAYOUT.byteSize();
        final long valueSize = readable.get(META_LAYOUT, offset[0] + keySize);
        // Без учета размеров объектов
        if (limited > 0 && limited - keySize - valueSize < offset[0]) {
            return null;
        }
        final MemorySegment key   = readMemorySegment(readable, offset, keySize);
        offset[0] += META_LAYOUT.byteSize();
        final MemorySegment value = readMemorySegment(readable, offset, valueSize);
        return new BaseEntry<>(key, value);
    }

    private Entry<MemorySegment> readEntry(final MemorySegment readable, final long[] offset) {
        return readEntry(readable, offset, 0);
    }

    public SortedMap<MemorySegment, Entry<MemorySegment>> load(final long limit) throws IOException {
        final SortedMap<MemorySegment, Entry<MemorySegment>> map = new TreeMap<>(COMPARATOR);
        if (Files.notExists(path)) {
            return map;
        }

        try (final FileChannel readerChannel = FileChannel.open(
                path,
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
                final Entry<MemorySegment> entry = readEntry(memorySegment, offset, limit);
                if (entry == null) {
                    break;
                }
                map.put(entry.key(), entry);
            }
            return map;
        }
    }

    public SortedMap<MemorySegment, Entry<MemorySegment>> load() throws IOException {
        return load(0);
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

    public Entry<MemorySegment> get(final MemorySegment key) throws IOException {
        if (Files.notExists(path)) {
            return null;
        }

        try (
                final Arena arena = Arena.ofConfined();
                final FileChannel readerChannel = FileChannel.open(path, StandardOpenOption.READ)
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
                if (UtilsMemorySegment.compare(key, entry.key()) == 0) {
                    return mapEntry(
                            path,
                            memorySegment.segmentOffset(entry.key()),
                            entry.key().byteSize(),
                            entry.value().byteSize()
                    );
                }
            }
        }
        return null;
    }

    private static long getTotalMapSize(final Map<MemorySegment, Entry<MemorySegment>> map) {
        long totalSize = ENTRY_META_SIZE * map.size();
        for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : map.entrySet()) {
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

    public void write(final SortedMap<MemorySegment, Entry<MemorySegment>> map) throws IOException {
        try (Arena arena = Arena.ofConfined(); FileChannel writerChannel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)
        ) {
            long offset = 0;
            final MemorySegment memorySegment = writerChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    offset,
                    getTotalMapSize(map),
                    arena
            );
            for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : map.entrySet()) {
                offset = putEntry(memorySegment, offset, entry);
            }
        }
    }
}
