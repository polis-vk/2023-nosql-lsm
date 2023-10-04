package ru.vk.itmo.volkovnikita;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final String PATH_SSTABLE = "sstable.txt";
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntries;
    private final Arena arena;
    private final Path basePath;
    public DaoImpl(Config config) {
        memorySegmentEntries = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        arena = Arena.ofConfined();
        basePath = config.basePath().resolve(PATH_SSTABLE);
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
            MemorySegment mapSegment = channel.map(
                    FileChannel.MapMode.READ_ONLY, 0, Files.size(basePath),
                    Arena.ofShared());

            long startOffset = 0L;
            long keyLength;
            long valueLength;

            while (startOffset < mapSegment.byteSize()) {
                keyLength = mapSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, startOffset);
                MemorySegment segmentKey = mapSegment.asSlice(startOffset + Long.BYTES, keyLength);
                startOffset += keyLength + Long.BYTES;
                valueLength = mapSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, startOffset);
                if (segmentKey.mismatch(key) == -1) {
                    return new BaseEntry<>(segmentKey, mapSegment.asSlice(startOffset + Long.BYTES, valueLength));
                }
                startOffset += valueLength + Long.BYTES;
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
            return memorySegmentEntries.tailMap(from).values().iterator();
        }

        return memorySegmentEntries.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegmentEntries.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return memorySegmentEntries.values().iterator();
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();

        long mappedMemorySize =
                memorySegmentEntries.values().stream().mapToLong(e -> e.key().byteSize() + e.value().byteSize()).sum();
        mappedMemorySize += Long.BYTES * memorySegmentEntries.size() * 2L;
        MemorySegment map;
        try(FileChannel fileChannel = FileChannel.open(basePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);
                Arena writeArena = Arena.ofConfined()) {
            map = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mappedMemorySize, writeArena);
        }

        save(map);
    }

    private void save(MemorySegment writeMappedMemorySegment) {
        long startOffSet = 0L;
        for (Entry<MemorySegment> entry : memorySegmentEntries.values()) {
            writeMappedMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, startOffSet, entry.key().byteSize());
            startOffSet += Long.BYTES;
            writeMappedMemorySegment.asSlice(startOffSet).copyFrom(entry.key());
            startOffSet += entry.key().byteSize();
            writeMappedMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, startOffSet, entry.value().byteSize());
            startOffSet += Long.BYTES;
            writeMappedMemorySegment.asSlice(startOffSet).copyFrom(entry.value());
            startOffSet += entry.value().byteSize();
        }
    }
}
