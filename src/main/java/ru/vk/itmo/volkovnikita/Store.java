package ru.vk.itmo.volkovnikita;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

public class Store implements Closeable {

    private final Path basePath;
    private static final String FILE_PATH = "data";
    private static final String EXTENSION = ".db";
    private final Arena arena;
    private final List<MemorySegment> readSegments;

    public Store(Config config) throws IOException {
        this.basePath = config.basePath().resolve(FILE_PATH);
        this.readSegments = new ArrayList<>();
        this.arena = Arena.ofShared();

        boolean created = true;
        try (var files = Files.list(basePath)) {
            files
                    .filter(p -> p.getFileName().toString().startsWith(FILE_PATH))
                    .filter(p -> p.getFileName().toString().endsWith(EXTENSION))
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try (FileChannel channel = FileChannel.open(p, StandardOpenOption.READ)) {
                            MemorySegment segment = channel.map(
                                    FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena
                            );
                            readSegments.add(segment);
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    });
        } catch (NoSuchFileException e) {
            created = false;
        } finally {
            if (!created) {
                arena.close();
            }
        }
    }


    public void saveMemoryData(NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntries)
            throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        arena.close();

        long mappedMemorySize =
                memorySegmentEntries.values().stream().mapToLong(e -> e.key().byteSize() + e.value().byteSize()).sum()
                        + Long.BYTES * memorySegmentEntries.size() * 2L;

        try (FileChannel fileChannel = FileChannel.open(basePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);
             Arena writeArena = Arena.ofConfined()) {
            MemorySegment seg = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mappedMemorySize, writeArena);

            long offset = 0L;

            for (Entry<MemorySegment> entry : memorySegmentEntries.values()) {
                MemorySegment key = entry.key();
                seg.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, key.byteSize());
                offset += Long.BYTES;
                MemorySegment.copy(key, 0, seg, offset, key.byteSize());
                offset += entry.key().byteSize();

                MemorySegment value = entry.value();
                seg.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value.byteSize());
                offset += Long.BYTES;
                MemorySegment.copy(value, 0, seg, offset, value.byteSize());
                offset += entry.value().byteSize();
            }
        }
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
        for (MemorySegment sstable : readSegments) {
            Iterator<Entry<MemorySegment>> iterator = iterateThroughSSTable(sstable, from, to);
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

    private long binarySearchUpperBoundOrEquals(MemorySegment ssTable, MemorySegment key) {
        long left = 0;
        long right = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        if (key == null) {
            return right;
        }
        right--;
        while (left <= right) {
            long mid = (left + right) / 2;

            long offset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES * mid);
            long keySize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            int cmp = MemorySegmentComparator.compareOffsets(offset, offset + keySize, ssTable, key);
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
        long offset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES * index);
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
}
