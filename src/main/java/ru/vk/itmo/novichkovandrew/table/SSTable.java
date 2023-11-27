package ru.vk.itmo.novichkovandrew.table;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.Utils;
import ru.vk.itmo.novichkovandrew.exceptions.FileChannelException;
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
    /**
     * Constable size of SSTable.
     */
    private final int size;

    /**
     * Unique number of this SST.
     */
    private final int sstNumber;
    private final Arena arena;

    public SSTable(Path path, int sstNumber) {
        try {
            this.sstChannel = FileChannel.open(path, StandardOpenOption.READ);
            this.size = Math.toIntExact(Utils.readLong(sstChannel, 0L));
            this.sstNumber = sstNumber;
            this.arena = Arena.ofShared();
        } catch (IOException ex) {
            throw new FileChannelException("Couldn't create FileChannel by path" + path, ex);
        }
    }

    @Override
    public void close() {
        try {
            this.sstChannel.close();
            if (arena.scope().isAlive()) {
                arena.close();
            }
        } catch (IOException e) {
            throw new FileChannelException("Couldn't close file channel " + sstChannel, e);
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
            int start = (from == null ? 0 : binarySearch(from)) + Boolean.compare(!fromInclusive, false);
            final int end = (to == null ? size : binarySearch(to)) + Boolean.compare(toInclusive, true);

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
                return new BaseEntry<>(getKeyByIndex(start), getValueByIndex(start++));
            }
        };
    }

    private MemorySegment getKeyByIndex(int index) {
        Objects.checkIndex(index, size);
        long keyOffset = getKeyOffset(index);
        long valueOffset = Math.abs(getValueOffset(index));
        return copyToArena(keyOffset, valueOffset);
    }

    private MemorySegment getValueByIndex(int index) {
        Objects.checkIndex(index, size);
        long valueOffset = getValueOffset(index);
        if (valueOffset < 0) {
            return null;
        }
        long nextKeyOffset = getKeyOffset(index + 1);
        return copyToArena(valueOffset, nextKeyOffset);
    }

    private MemorySegment copyToArena(long valOffset, long nextOffset) {
        try (Arena mapArena = Arena.ofShared()) {
            var mappedMem = sstChannel.map(FileChannel.MapMode.READ_ONLY, valOffset, nextOffset - valOffset, mapArena);
            var nativeMem = arena.allocate(mappedMem.byteSize());
            Utils.copyToSegment(nativeMem, mappedMem, 0);
            return nativeMem;
        } catch (IOException ex) {
            throw new FileChannelException("Couldn't map file from channel " + sstChannel, ex);
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
