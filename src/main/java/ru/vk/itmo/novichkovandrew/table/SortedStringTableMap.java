package ru.vk.itmo.novichkovandrew.table;

import ru.vk.itmo.novichkovandrew.Cell;
import ru.vk.itmo.novichkovandrew.MemorySegmentCell;
import ru.vk.itmo.novichkovandrew.Utils;
import ru.vk.itmo.novichkovandrew.iterator.PeekTableIterator;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;

public class SortedStringTableMap implements TableMap<MemorySegment, MemorySegment> {

    private final FileChannel sstChannel;
    private final Arena arena;
    /**
     * Constable size of SSTable.
     */
    private final int size;

    /**
     * Unique number of this SST.
     */
    private final int sstNumber;

    private final Comparator<MemorySegment> comparator;

    public SortedStringTableMap(Path path, int sstNumber, Comparator<MemorySegment> comparator,
                                Arena arena) {
        try {
            this.sstChannel = FileChannel.open(path, StandardOpenOption.READ);
            this.size = Math.toIntExact(Utils.readLong(sstChannel, 0L));
            this.sstNumber = sstNumber;
            this.comparator = comparator;
            this.arena = arena;
        } catch (IOException ex) {
            throw new RuntimeException("Couldn't create FileChannel by path" + path);
        }
    }

    @Override
    public void close() {
        try {
            this.sstChannel.close();
        } catch (IOException e) {
            System.err.printf("Couldn't close file channel: %s%n", sstChannel);
        }
    }


    @Override
    public MemorySegment ceilKey(MemorySegment key) {
        int index = binarySearch(key);
        return getKeyByIndex(index);
    }


    @Override
    public Cell<MemorySegment> getCell(MemorySegment key) {
        int index = binarySearch(key);
        MemorySegment rawKey = getKeyByIndex(index);
        if (comparator.compare(rawKey, key) != 0) {
            return null;
        }
        MemorySegment value = getValueByIndex(index);
        return new MemorySegmentCell(rawKey, value);
    }


    @Override
    public int size() {
        return size;
    }

    @Override
    public PeekTableIterator<MemorySegment> keyIterator(MemorySegment from, MemorySegment to) {
        Iterator<MemorySegment> iterator = new Iterator<>() {
            int index = binarySearch(from);
            MemorySegment current = getIfRanged(index);

            private MemorySegment getIfRanged(int index) {
                if (index >= size) return null;
                MemorySegment value = getKeyByIndex(index);
                return comparator.compare(from, value) <= 0 && comparator.compare(value, to) < 0 ? value : null;
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public MemorySegment next() {
                MemorySegment key = current;
                index++;
                current = getIfRanged(index);
                return key;
            }
        };
        return new PeekTableIterator<>(iterator, sstNumber);
    }


    private MemorySegment getKeyByIndex(int index) {
        Objects.checkIndex(index, size);
        long keyOffset = getKeyOffset(index);
        long valueOffset = Math.abs(getValueOffset(index));
        try {
            return sstChannel.map(FileChannel.MapMode.READ_ONLY, keyOffset, valueOffset - keyOffset, arena);
        } catch (IOException ex) {
            throw new RuntimeException(
                    String.format("Couldn't map file from channel %s: %s", sstChannel, ex.getMessage())
            );
        }
    }

    private MemorySegment getValueByIndex(int index) {
        Objects.checkIndex(index, size);
        long valueOffset = getValueOffset(index);
        if (valueOffset < 0) {
            return null;
        }
        long nextKeyOffset = getKeyOffset(index + 1);
        try {
            return sstChannel.map(FileChannel.MapMode.READ_ONLY, valueOffset, nextKeyOffset - valueOffset, arena);
        } catch (IOException ex) {
            throw new RuntimeException(
                    String.format("Couldn't map file from channel %s: %s", sstChannel, ex.getMessage())
            );
        }
    }

    private int binarySearch(MemorySegment key) {
        int l = 0;
        int r = size - 1;
        while (l < r) {
            int mid = l + (r - l) / 2;
            MemorySegment middle = getKeyByIndex(mid);
            if (comparator.compare(key, middle) <= 0) {
                r = mid;
            } else {
                l = mid + 1;
            }
        }
        return l;
    }

    private long getKeyOffset(int index) {
        long rawOffset = (2L * index + 1) * (long) Long.BYTES;
        return Utils.readLong(sstChannel, rawOffset);
    }

    private long getValueOffset(int index) {
        long rawOffset = (2L * index + 2) * (long) Long.BYTES;
        return Utils.readLong(sstChannel, rawOffset);
    }
}
