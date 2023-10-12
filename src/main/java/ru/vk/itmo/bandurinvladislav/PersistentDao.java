package ru.vk.itmo.bandurinvladislav;

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
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
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
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> inMemoryStorage =
            new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);
    private final Path persistentStorage;
    private final MemorySegment fileSegment;

    public PersistentDao(Config config) {
        persistentStorage = config.basePath().resolve(STORAGE_NAME);
        try (FileChannel fileChannel = FileChannel.open(persistentStorage,
                StandardOpenOption.READ)) {
            fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    fileChannel.size(),
                    daoArena
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

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
        }

        long offset = 0;
        long entryCount = fileSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;

        for (long i = 0; i < entryCount; i++) {
            long keySize = fileSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            if (keySize != key.byteSize()) {
                offset += Long.BYTES + keySize;
                offset += Long.BYTES + fileSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                continue;
            }

            offset += Long.BYTES;
            MemorySegment entryKey = fileSegment.asSlice(offset, keySize);
            offset += keySize;

            long valueSize = fileSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            if (key.mismatch(entryKey) == -1) {
                MemorySegment entryValue = fileSegment.asSlice(offset, valueSize);
                return new BaseEntry<>(entryKey, entryValue);
            } else {
                offset += valueSize;
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

    private void writeMemorySegment(MemorySegment fileSegment, MemorySegment data, long offset) {
        fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, data.byteSize());
        MemorySegment.copy(data, 0, fileSegment, offset + Long.BYTES, data.byteSize());
    }

    @Override
    public void flush() throws IOException {
        long writeOffset = 0;
        try (FileChannel fileChannel = FileChannel.open(persistentStorage,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.TRUNCATE_EXISTING);
             Arena arena = Arena.ofConfined()) {

            long fileSegmentSize = Long.BYTES;
            for (Map.Entry<MemorySegment, Entry<MemorySegment>> e : inMemoryStorage.entrySet()) {
                fileSegmentSize += e.getKey().byteSize() + e.getValue().value().byteSize() + 2 * Long.BYTES;
            }

            MemorySegment fileWriteSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    fileSegmentSize,
                    arena
            );

            fileWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, writeOffset, inMemoryStorage.size());
            writeOffset += 8;

            for (Map.Entry<MemorySegment, Entry<MemorySegment>> e : inMemoryStorage.entrySet()) {
                writeMemorySegment(fileWriteSegment, e.getKey(), writeOffset);
                writeOffset += Long.BYTES + e.getKey().byteSize();
                writeMemorySegment(fileWriteSegment, e.getValue().value(), writeOffset);
                writeOffset += Long.BYTES + e.getValue().value().byteSize();
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
