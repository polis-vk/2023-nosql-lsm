package ru.vk.itmo.novichkovandrew.dao;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.Utils;
import ru.vk.itmo.novichkovandrew.exceptions.FileChannelException;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PersistentDao extends InMemoryDao {
    /**
     * File with SSTable path.
     */
    private final Path path;

    private final Arena arena;
    private final StandardOpenOption[] openOptions = new StandardOpenOption[]{
            StandardOpenOption.WRITE,
            StandardOpenOption.READ,
            StandardOpenOption.CREATE
    };

    public PersistentDao(Path path) {
        this.path = path.resolve("data.txt");
        this.arena = Arena.ofConfined();
    }

    @Override
    public void flush() throws IOException {
        try (FileChannel sst = FileChannel.open(path, openOptions)) {
            long metaSize = super.getMetaDataSize();
            long sstOffset = 0L;
            long indexOffset = Utils.writeLong(sst, 0L, entriesMap.size());
            MemorySegment sstMap = sst.map(FileChannel.MapMode.READ_WRITE, metaSize, tableByteSize.get(), arena);
            for (Entry<MemorySegment> entry : entriesMap.values()) {
                long keyOffset = sstOffset + metaSize;
                long valueOffset = keyOffset + entry.key().byteSize();
                indexOffset = writePosToFile(sst, indexOffset, keyOffset, valueOffset);
                sstOffset = Utils.copyToSegment(sstMap, entry.key(), sstOffset);
                sstOffset = Utils.copyToSegment(sstMap, entry.value(), sstOffset);
            }
            writePosToFile(sst, indexOffset, sstOffset + metaSize, 0L);
        }
    }

    private long writePosToFile(FileChannel channel, long rawOffset, long keyOff, long valOff) throws IOException {
        long offset = rawOffset;
        offset = Utils.writeLong(channel, offset, keyOff);
        offset = Utils.writeLong(channel, offset, valOff);
        return offset;
    }

    @Override
    public void close() throws IOException {
        if (!entriesMap.isEmpty()) flush();
        if (arena.scope().isAlive()) {
            arena.close();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = super.get(key);
        if (entry != null) {
            return entry;
        }
        if (Files.notExists(path)) {
            return null;
        }
        try (FileChannel sstChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            return binarySearch(sstChannel, key);
        } catch (IOException e) {
            throw new FileChannelException("Couldn't open file " + path, e);
        }
    }

    private Entry<MemorySegment> binarySearch(FileChannel sst, MemorySegment key) throws IOException {
        int l = 0;
        int r = Math.toIntExact(Utils.readLong(sst, 0L));
        while (l < r) {
            int mid = l + (r - l) / 2;
            MemorySegment middle = getKeyByIndex(sst, mid);
            if (comparator.compare(key, middle) <= 0) {
                r = mid;
            } else {
                l = mid + 1;
            }
        }
        var resultKey = getKeyByIndex(sst, l);
        if (comparator.compare(key, resultKey) == 0) {
            return new BaseEntry<>(resultKey, getValueByIndex(sst, l));
        }
        return null;
    }

    private MemorySegment getKeyByIndex(FileChannel sst, int index) throws IOException {
        long keyOffset = getKeyOffset(sst, index);
        long valueOffset = getValueOffset(sst, index);
        if (valueOffset == 0) return null;
        return sst.map(FileChannel.MapMode.READ_ONLY, keyOffset, valueOffset - keyOffset, arena);
    }

    private MemorySegment getValueByIndex(FileChannel sst, int index) throws IOException {
        long valueOffset = getValueOffset(sst, index);
        if (valueOffset < 0) {
            return null;
        }
        long nextKeyOffset = getKeyOffset(sst, index + 1);
        return sst.map(FileChannel.MapMode.READ_ONLY, valueOffset, nextKeyOffset - valueOffset, arena);
    }

    private long getKeyOffset(FileChannel sst, int index) throws IOException {
        long rawOffset = (2L * index + 1) * (long) Long.BYTES;
        return Utils.readLong(sst, rawOffset);
    }

    private long getValueOffset(FileChannel sst, int index) throws IOException {
        long rawOffset = (2L * index + 2) * (long) Long.BYTES;
        return Utils.readLong(sst, rawOffset);
    }
}
