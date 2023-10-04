package ru.vk.itmo.bandurinvladislav;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> MEMORY_SEGMENT_COMPARATOR = (m1, m2) -> {
        long mismatch = m1.mismatch(m2);
        if (mismatch == m2.byteSize()) {
            return 1;
        } else if (mismatch == m1.byteSize()) {
            return -1;
        } else if (mismatch == -1) {
            return 0;
        } else {
            return m1.get(ValueLayout.JAVA_BYTE, mismatch) - m2.get(ValueLayout.JAVA_BYTE, mismatch);
        }
    };
    private static final String STORAGE_NAME = "persistentStorage.txt";

    private final Arena daoArena = Arena.ofConfined();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> inMemoryStorage = new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);
    private final Path persistentStorage;

    public InMemoryDao(Config config) throws IOException {
        persistentStorage = config.basePath().resolve(STORAGE_NAME);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return getEntryIterator(from, to);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> memorySegmentEntry = inMemoryStorage.get(key);
        if (memorySegmentEntry != null) {
            return memorySegmentEntry;
        }

        if (!Files.exists(persistentStorage)) {
            return null;
        } else {
            try (FileChannel fileChannel = FileChannel.open(persistentStorage,
                    StandardOpenOption.READ)) {
                ByteBuffer sizeBuffer = ByteBuffer.allocate(8);
                fileChannel.read(sizeBuffer);
                long entryCount = sizeBuffer.position(0).getLong();
                for (long i = 0; i < entryCount; i++) {
                    MemorySegment entryKey = readMemorySegment(fileChannel, sizeBuffer, daoArena);
                    MemorySegment entryValue = readMemorySegment(fileChannel, sizeBuffer, daoArena);
                    if (MEMORY_SEGMENT_COMPARATOR.compare(key, entryKey) == 0) {
                        return new BaseEntry<>(entryKey, entryValue);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        inMemoryStorage.put(entry.key(), entry);
    }

    private Iterator<Entry<MemorySegment>> getEntryIterator(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return inMemoryStorage.values().iterator();
        } else if (from == null) {
            return inMemoryStorage.headMap(to, false).values().iterator();
        } else if (to == null) {
            return inMemoryStorage.tailMap(from, true).values().iterator();
        } else {
            return inMemoryStorage.subMap(from, true, to, false).values().iterator();
        }
    }

    private void writeMemorySegment(FileChannel fileChannel, ByteBuffer sizeBuffer, MemorySegment value, Arena arena)
            throws IOException {
        fileChannel.write(sizeBuffer.putLong(0, value.byteSize()).position(0));
        MemorySegment offHeapSegment = fileChannel.map(FileChannel.MapMode.READ_WRITE, fileChannel.position(), value.byteSize(), arena);
        offHeapSegment.copyFrom(value);
        offHeapSegment.load();
        fileChannel.position(fileChannel.position() + value.byteSize());
    }

    private MemorySegment readMemorySegment(FileChannel fileChannel, ByteBuffer sizeBuffer, Arena arena) throws IOException {
        sizeBuffer.position(0);
        fileChannel.read(sizeBuffer);
        MemorySegment fileSegment = fileChannel.map(
                FileChannel.MapMode.READ_ONLY,
                fileChannel.position(),
                sizeBuffer.position(0).getLong(),
                arena
        );
        fileChannel.position(fileChannel.position() + fileSegment.byteSize());
        return fileSegment;
    }

    @Override
    public void flush() throws IOException {
        int entryCount = inMemoryStorage.size();

        try (FileChannel fileChannel = FileChannel.open(persistentStorage,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.TRUNCATE_EXISTING);
             Arena writeArena = Arena.ofConfined()) {
            ByteBuffer sizeBuffer = ByteBuffer.allocate(8);
            fileChannel.write(sizeBuffer.putLong(0, entryCount));
            for (Map.Entry<MemorySegment, Entry<MemorySegment>> memorySegmentEntry : inMemoryStorage.entrySet()) {
                writeMemorySegment(fileChannel, sizeBuffer, memorySegmentEntry.getKey(), writeArena);
                writeMemorySegment(fileChannel, sizeBuffer, memorySegmentEntry.getValue().value(), writeArena);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (daoArena.scope().isAlive()) {
            daoArena.close();
        }

        flush();
    }
}
