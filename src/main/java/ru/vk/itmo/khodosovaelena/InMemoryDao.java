package ru.vk.itmo.khodosovaelena;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntries
            = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    private final Arena arena;
    private final Path basePath;

    public InMemoryDao(Config config) {
        basePath = config.basePath().resolve("sstable.txt");
        arena = Arena.ofConfined();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (memorySegmentEntries.containsKey(key)) {
            return memorySegmentEntries.get(key);
        }

        if (basePath == null || !Files.exists(basePath)) {
            return null;
        }

        try (FileChannel channel = FileChannel.open(basePath, StandardOpenOption.READ)) {
            MemorySegment mappedSegment = channel.map(
                    FileChannel.MapMode.READ_ONLY, 0, Files.size(basePath),
                    Arena.ofShared());

            long offset = 0L;
            long keySize;
            long valueSize;
            while (offset < mappedSegment.byteSize()) {
                keySize = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                MemorySegment segmentKey = mappedSegment.asSlice(offset + Long.BYTES, keySize);
                offset += keySize + Long.BYTES;
                valueSize = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                if (segmentKey.mismatch(key) == -1) {
                    return new BaseEntry<>(segmentKey, mappedSegment.asSlice(offset + Long.BYTES, valueSize));
                }
                offset += valueSize + Long.BYTES;
            }
            return null;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memorySegmentEntries.values().iterator();
        }
        if (from == null) {
            return memorySegmentEntries.headMap(to).values().iterator();
        }
        if (to == null) {
            return memorySegmentEntries.headMap(from).values().iterator();
        }
        return memorySegmentEntries.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegmentEntries.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        if (!arena.scope().isAlive()) {
            return;
        }
        saveState();
    }

    private void saveState() {
        long writeOffset = 0;
        try (FileChannel channel = FileChannel.open(basePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);
            Arena writeArena = Arena.ofConfined()) {
            MemorySegment mappedSegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, getMapSize(), writeArena);
            for (Entry<MemorySegment> entry : memorySegmentEntries.values()) {
                mappedSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, writeOffset, entry.key().byteSize());
                writeOffset += Long.BYTES;
                MemorySegment.copy(entry.key(), 0, mappedSegment, writeOffset, entry.key().byteSize());
                writeOffset += entry.key().byteSize();
                mappedSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, writeOffset, entry.value().byteSize());
                writeOffset += Long.BYTES;
                MemorySegment.copy(entry.value(), 0, mappedSegment, writeOffset, entry.value().byteSize());
                writeOffset += entry.value().byteSize();
            }
            mappedSegment.load();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private long getMapSize() {
        long mapSize = 0;
        for (Entry<MemorySegment> entry : memorySegmentEntries.values()) {
            mapSize += entry.key().byteSize() + entry.value().byteSize();
        }
        return mapSize + Long.BYTES * memorySegmentEntries.size() * 2L;
    }

    public static class MemorySegmentComparator implements Comparator<MemorySegment> {
        @Override
        public int compare(MemorySegment segment1, MemorySegment segment2) {
            long offset = segment1.mismatch(segment2);
            if (offset == -1) {
                return 0;
            } else if (offset == segment2.byteSize()) {
                return 1;
            } else if (offset == segment1.byteSize()) {
                return -1;
            }
            return Byte.compare(segment1.get(ValueLayout.JAVA_BYTE, offset),
                    segment2.get(ValueLayout.JAVA_BYTE, offset));
        }
    }
}
