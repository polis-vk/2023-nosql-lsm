package ru.vk.itmo.volkovnikita;

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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class Store implements Closeable {

    private static final String FILE_PATH = "data";
    private static final String EXTENSION = ".db";
    private final Arena arena;
    private final List<MemorySegment> readSegments;

    public Store(Config config) {
        Path path = config.basePath();

        List<MemorySegment> ssTables = new ArrayList<>();
        Arena arena = Arena.ofShared();

        try (var files = Files.list(path)) {
            files
                    .filter(p -> p.getFileName().toString().startsWith(FILE_PATH))
                    .filter(p -> p.getFileName().toString().endsWith(EXTENSION))
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try (FileChannel channel = FileChannel.open(p, StandardOpenOption.READ)) {
                            MemorySegment segment = channel.map(
                                    FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena
                            );
                            ssTables.add(segment);
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        this.arena = arena;
        this.readSegments = ssTables;
    }

    public void save(Config config, Collection<Entry<MemorySegment>> entries, Store store)
            throws IOException {
        if (store.arena.scope().isAlive()) {
            return;
        }

        int nextSSTable = store.readSegments.size();
        String indexWithZeroPadding = String.format("%010d", nextSSTable);
        Path path = config.basePath().resolve(FILE_PATH + indexWithZeroPadding + EXTENSION);

        long indicesSize = (long) Long.BYTES * entries.size();
        long sizeOfNewSSTable = indicesSize + Long.BYTES;
        for (Entry<MemorySegment> entry : entries) {
            sizeOfNewSSTable += 2 * Long.BYTES + entry.key().byteSize()
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

            long offsetIndex = Long.BYTES;
            long offsetData = indicesSize + Long.BYTES;
            for (Entry<MemorySegment> entry : entries) {
                newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, offsetData);
                offsetIndex += Long.BYTES;

                offsetData = saveEntrySegment(newSSTable, entry, offsetData);
            }

        }
    }

    public long saveEntrySegment(MemorySegment newSSTable, Entry<MemorySegment> entry, long offsetData) {
        long newOffset = offsetData;
        newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, newOffset, entry.key().byteSize());
        newOffset += Long.BYTES;

        MemorySegment.copy(entry.key(), 0, newSSTable, newOffset, entry.key().byteSize());
        newOffset += entry.key().byteSize();

        if (entry.value() == null) {
            newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, newOffset, -1);
            newOffset += Long.BYTES;
        } else {
            newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, newOffset, entry.value().byteSize());
            newOffset += Long.BYTES;
            MemorySegment.copy(entry.value(), 0, newSSTable, newOffset, entry.value().byteSize());
            newOffset += entry.value().byteSize();
        }
        return newOffset;
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

            long offset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + Long.BYTES * mid);
            long keySize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            int cmp = MemorySegmentComparator.compareOffsets(offset, offset + keySize,
                    ssTable, key
            );
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
        long offset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + Long.BYTES * index);
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
        List<IndexIterator> peekIterators = new ArrayList<>();
        peekIterators.add(new IndexIterator(0, memoryIterator));
        int order = 1;
        for (MemorySegment segment : readSegments) {
            Iterator<Entry<MemorySegment>> iterator = iterateThroughSSTable(segment, from, to);
            peekIterators.add(new IndexIterator(order, iterator));
            order++;
        }
        return new MergeIterator(peekIterators);
    }

    @Override
    public void close() {
        if (arena.scope().isAlive()) {
            arena.close();
        }
    }
}
