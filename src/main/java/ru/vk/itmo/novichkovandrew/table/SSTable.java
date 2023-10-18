package ru.vk.itmo.novichkovandrew.table;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.Utils;
import ru.vk.itmo.novichkovandrew.iterator.TableIterator;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class SSTable extends AbstractTable {

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

    public SSTable(Path path, int sstNumber) {
        try {
            this.sstChannel = FileChannel.open(path, StandardOpenOption.READ);
            this.size = Math.toIntExact(Utils.readLong(sstChannel, 0L));
            this.sstNumber = sstNumber;
            this.arena = Arena.ofConfined();
        } catch (IOException ex) {
            throw new RuntimeException("Couldn't create FileChannel by path" + path);
        }
    }

    @Override
    public void close() {
        try {
            this.sstChannel.close();
            this.arena.close();
        } catch (IOException e) {
            System.err.printf("Couldn't close file channel: %s%n", sstChannel);
        }
    }


    @Override
    public int size() {
        return size;
    }


    @Override
    public TableIterator<MemorySegment> tableIterator(MemorySegment from, boolean fromInclusive,
                                                      MemorySegment to, boolean toInclusive) {
        return new TableIterator<>() {
            int start = setFrom();
            final int end = setTo();

            @Override
            public int getTableNumber() {
                return sstNumber;
            }

            @Override
            public boolean hasNext() {
                return start < size && start <= end;
            }

            @Override
            public Entry<MemorySegment> next() {
                MemorySegment key = getKeyByIndex(start);
                MemorySegment value = getValueByIndex(start);
                start++;
                return new BaseEntry<>(key, value);
            }

            private int setFrom() {
                int position = from == null ? 0 : binarySearch(from);
                if (!fromInclusive && comparator.compare(getKeyByIndex(position), from) == 0) {
                    position++;
                }
                return position;
            }

            private int setTo() {
                int position = to == null ? size : binarySearch(to);
                return position + Boolean.compare(toInclusive, true); // TODO: reverse?
            }
        };
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
        while (l <= r) {
            int mid = l + (r - l) / 2;
            MemorySegment middle = getKeyByIndex(mid);
            if (comparator.compare(key, middle) <= 0) {
                r = mid - 1;
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
