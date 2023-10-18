package ru.vk.itmo.prokopyevnikita;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class Storage implements Closeable {

    private static final String DB_PREFIX = "data";
    private static final String DB_EXTENSION = ".db";
    private static final long FILE_PREFIX = Long.BYTES;
    private final Arena arena;
    private final List<MemorySegment> ssTables;

    private Storage(Arena arena, List<MemorySegment> ssTables) {
        this.arena = arena;
        this.ssTables = ssTables;
    }

    public static Storage load(Config config) throws IOException {
        Path path = config.basePath();

        List<MemorySegment> ssTables = new ArrayList<>();
        Arena arena = Arena.ofShared();

        for (int i = 0; ; i++) {
            Path p = path.resolve(DB_PREFIX + i + DB_EXTENSION);
            if (Files.exists(p)) {
                try (FileChannel channel = FileChannel.open(p, StandardOpenOption.READ)) {
                    MemorySegment segment = channel.map(
                            FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena
                    );
                    ssTables.add(segment);
                } catch (IOException e) {
                    throw new IllegalStateException("Can't open SSTable", e);
                }
            } else {
                break;
            }
        }

        Collections.reverse(ssTables);
        return new Storage(arena, ssTables);
    }

    public static void save(Config config, Collection<Entry<MemorySegment>> entries, Storage storage) throws IOException {
        if (storage.arena.scope().isAlive()) {
            throw new IllegalStateException("Previous arena is alive");
        }

        if (entries.isEmpty()) {
            return;
        }

        int nextSSTable = storage.ssTables.size();
        Path path = config.basePath().resolve(DB_PREFIX + nextSSTable + DB_EXTENSION);

        long indicesSize = (long) Long.BYTES * entries.size();
        long sizeOfNewSSTable = indicesSize + FILE_PREFIX;
        for (Entry<MemorySegment> entry : entries) {
            sizeOfNewSSTable +=
                    2 * Long.BYTES
                            + entry.key().byteSize()
                            + (entry.value() == null ? 0 : entry.value().byteSize());
        }

        try (Arena arenaSave = Arena.ofConfined();
             var channel = FileChannel.open(
                     path,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.TRUNCATE_EXISTING)
        ) {

            MemorySegment newSSTable = channel.map(FileChannel.MapMode.READ_WRITE, 0, sizeOfNewSSTable, arenaSave);

            newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, entries.size());

            long offsetIndex = FILE_PREFIX;
            long offsetData = indicesSize + FILE_PREFIX;
            for (Entry<MemorySegment> entry : entries) {
                newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, offsetData);
                offsetIndex += Long.BYTES;

                offsetData = saveEntrySegment(newSSTable, entry, offsetData);
            }

        }
    }

    public static long saveEntrySegment(MemorySegment newSSTable, Entry<MemorySegment> entry, long offsetData) {
        long offset = offsetData;
        newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
        offset += Long.BYTES;

        MemorySegment.copy(entry.key(), 0, newSSTable, offset, entry.key().byteSize());
        offset += entry.key().byteSize();

        if (entry.value() == null) {
            newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, -1);
            offset += Long.BYTES;
        } else {
            newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
            offset += Long.BYTES;

            MemorySegment.copy(entry.value(), 0, newSSTable, offset, entry.value().byteSize());
            offset += entry.value().byteSize();
        }
        return offset;
    }

    private long binarySearchUpperBoundOrEquals(MemorySegment ssTable, MemorySegment key) {
        long left = 0;
        long right = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        if (key == null) {
            return right;
        }
        right--;
        while (left <= right) {
            long mid = (left + right) / 2;

            long offset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, FILE_PREFIX + Long.BYTES * mid);
            long keySize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            int cmp = MemorySegmentComparator.compareWithOffsets(
                    ssTable, offset, offset + keySize,
                    key);
            if (cmp < 0) {
                left = mid + 1;
            } else if (cmp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    private Entry<MemorySegment> getEntryByIndex(MemorySegment ssTable, long index) {
        long offset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, FILE_PREFIX + Long.BYTES * index);
        long keySize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);

        MemorySegment keySegment = ssTable.asSlice(offset + Long.BYTES, keySize);
        offset += Long.BYTES + keySize;

        long valueSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;

        if (valueSize == -1) {
            return new BaseEntry<>(keySegment, null);
        }

        MemorySegment valueSegment = ssTable.asSlice(offset, valueSize);
        return new BaseEntry<>(keySegment, valueSegment);
    }

    public Iterator<Entry<MemorySegment>> iterateThroughSSTable(
            MemorySegment ssTable,
            MemorySegment from,
            MemorySegment to
    ) {
        long left = binarySearchUpperBoundOrEquals(ssTable, from);
        long right = binarySearchUpperBoundOrEquals(ssTable, to);

        return new Iterator<>() {
            long current = left;

            @Override
            public boolean hasNext() {
                return current < right;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No next element");
                }
                return getEntryByIndex(ssTable, current++);
            }
        };
    }

    public Iterator<Entry<MemorySegment>> getIterator(
            MemorySegment from,
            MemorySegment to,
            Iterator<Entry<MemorySegment>> memoryIterator
    ) {
        List<OrderedPeekIterator<Entry<MemorySegment>>> peekIterators = new ArrayList<>();
        peekIterators.add(new OrderedPeekIteratorImpl(0, memoryIterator));
        int order = 1;
        for (MemorySegment sstable : ssTables) {
            Iterator<Entry<MemorySegment>> iterator = iterateThroughSSTable(sstable, from, to);
            peekIterators.add(new OrderedPeekIteratorImpl(order, iterator));
            order++;
        }
        return new MergeSkipNullValuesIterator(peekIterators);
    }

    @Override
    public void close() {
        if (arena.scope().isAlive()) {
            arena.close();
        }
    }
}
