package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class SSTable {

    private static final long INDEX_ENTRY_SIZE = 2 * ValueLayout.JAVA_LONG.byteSize();
    public static final Comparator<MemorySegment> COMPARATOR = UtilsMemorySegment::compare;

    private final MemorySegment dataSegment;
    private final IndexList indexList;

    private class IndexList extends AbstractList<MemorySegment> {

        public static long MAX_BYTE_SIZE = Integer.MAX_VALUE * INDEX_ENTRY_SIZE;

        private final MemorySegment segment;
        private final long valuesOffset;

        public IndexList(MemorySegment segment) {
            if (segment.byteSize() > MAX_BYTE_SIZE) {
                segment = segment.asSlice(0, MAX_BYTE_SIZE);
            }
            this.segment = segment;
            valuesOffset = size() > 0 ? readMeta(0).valueOffset : 0;
        }

        private long getEntryOffset(final int index) {
            if (size() <= index) {
                return -1;
            }
            return INDEX_ENTRY_SIZE * index;
        }

        private long readOffset(final long offset) {
            return segment.get(ValueLayout.JAVA_LONG, offset);
        }

        private EntryMeta readMeta(final int index) {
            final long offset = getEntryOffset(index);
            final long nextEntryOffset = getEntryOffset(index + 1);

            final long keyOffset = readOffset(offset);
            final long valueOffset = readOffset(offset + ValueLayout.JAVA_LONG.byteSize());

            long keySize = valuesOffset - keyOffset;
            long valueSize = dataSegment.byteSize() - valueOffset;
            if (nextEntryOffset != -1) {
                keySize = readOffset(nextEntryOffset) - keyOffset;
                valueSize = readOffset(nextEntryOffset + ValueLayout.JAVA_LONG.byteSize()) - valueOffset;
            }
            return new EntryMeta(keyOffset, keySize, valueOffset, valueSize);
        }

        private MemorySegment readKey(final EntryMeta meta) {
            return dataSegment.asSlice(meta.keyOffset, meta.keySize);
        }

        @Override
        public MemorySegment get(final int index) {
            return readKey(readMeta(index));
        }

        public Entry<MemorySegment> getEntry(final int index) {
            final EntryMeta meta = readMeta(index);
            final MemorySegment value = dataSegment.asSlice(meta.valueOffset, meta.valueSize);
            return new BaseEntry<>(readKey(meta), value);
        }

        @Override
        public int size() {
            return (int)(segment.byteSize() / INDEX_ENTRY_SIZE);
        }

        public static void write(
                final MemorySegment writer,
                final long[][] offsets
        ) {
            long offset = 0;
            for (final long[] entry: offsets) {
                writer.set(ValueLayout.JAVA_LONG, offset, entry[0]);
                writer.set(ValueLayout.JAVA_LONG, offset += ValueLayout.JAVA_LONG.byteSize(), entry[1]);
                offset += ValueLayout.JAVA_LONG.byteSize();
            }
        }
    }

    public SSTable(final Path root, final String name, final Arena arena) throws IOException {
        dataSegment = mapSegment(getDataPath(root, name), arena);
        indexList = new IndexList(mapSegment(getIndexPath(root, name), arena));
    }

    private static Path getDataPath(final Path root, final String name) {
        return root.resolve(name);
    }

    private static Path getIndexPath(final Path root, final String name) {
        return root.resolve(name + "_index");
    }

    private static MemorySegment mapSegment(final Path path, final Arena arena) throws IOException {
        try (FileChannel readerChannel = FileChannel.open(
                path,
                StandardOpenOption.READ)
        ) {
            return readerChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    readerChannel.size(),
                    arena
            );
        }
    }

    public Entry<MemorySegment> get(final MemorySegment key) throws IOException {
        final int pos = Collections.binarySearch(indexList, key, COMPARATOR);
        return pos >= 0 ? indexList.getEntry(pos) : null;
    }

    private static long getTotalMapSize(final SortedMap<MemorySegment, Entry<MemorySegment>> map) {
        long totalSize = 0;
        for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : map.entrySet()) {
            totalSize += entry.getKey().byteSize() + entry.getValue().value().byteSize();
        }
        return totalSize;
    }

    public static void write(
            final SortedMap<MemorySegment, Entry<MemorySegment>> map,
            final Path path,
            final String name
    ) throws IOException {
        long[][] offsets;
        try (Arena arena = Arena.ofConfined(); FileChannel writerChannel = FileChannel.open(
                getDataPath(path, name),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)
        ) {
            final MemorySegment memorySegment = writerChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    getTotalMapSize(map),
                    arena
            );
            offsets = new long[map.size()][2];

            int index = 0;
            long totalOffset = 0;
            for (final MemorySegment key : map.keySet()) {
                offsets[index++][0] = totalOffset;
                MemorySegment.copy(
                        key,
                        0,
                        memorySegment,
                        totalOffset,
                        key.byteSize()
                );
                totalOffset += key.byteSize();
            }

            index = 0;
            for (final Entry<MemorySegment> value : map.values()) {
                offsets[index++][1] = totalOffset;
                MemorySegment.copy(
                        value.value(),
                        0,
                        memorySegment,
                        totalOffset,
                        value.value().byteSize()
                );
                totalOffset += value.value().byteSize();
            }
        }

        try (Arena arena = Arena.ofConfined(); FileChannel writerChannel = FileChannel.open(
                getIndexPath(path, name),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)
        ) {
            final MemorySegment memorySegment = writerChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    INDEX_ENTRY_SIZE * map.size(),
                    arena
            );
            IndexList.write(memorySegment, offsets);
        }
    }

}
