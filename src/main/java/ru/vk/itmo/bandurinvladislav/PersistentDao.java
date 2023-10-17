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
    private static final String STORAGE_NAME = "persistentStorage";
    private static final String INDEX_NAME_SUFFIX = "-index";

    private final Arena daoArena = Arena.ofConfined();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> inMemoryStorage =
            new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);
    private final Path persistentStorage;
    private final MemorySegment fileSegment;
    private final MemorySegment indexSegment;

    public PersistentDao(Config config) {
        persistentStorage = config.basePath().resolve(STORAGE_NAME);
        Path indexPath = persistentStorage.resolve(INDEX_NAME_SUFFIX);
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

        try (FileChannel indexChannel = FileChannel.open(indexPath,
                StandardOpenOption.READ)) {
            indexSegment = indexChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    indexChannel.size(),
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

        long entryIndex = findEntryIndex(key);
        long keySize = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, entryIndex + Integer.BYTES);
        long valueSize = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, entryIndex + Integer.BYTES + Long.BYTES);
        long offsetInFileSegment = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, entryIndex + Integer.BYTES + 2 * Long.BYTES);
        MemorySegment entryKey = fileSegment.asSlice(offsetInFileSegment, keySize);

        if (entryKey.mismatch(key) == -1) {
            MemorySegment entryValue = fileSegment.asSlice(offsetInFileSegment + keySize, valueSize);
            return new BaseEntry<>(entryKey, entryValue);
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
        MemorySegment.copy(data, 0, fileSegment, offset, data.byteSize());
    }

    @Override
    public void flush() throws IOException {
        long storageOffset = 0;
        long indexOffset = 0;
        try (FileChannel fileChannel = FileChannel.open(persistentStorage,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel indexChannel = FileChannel.open(persistentStorage,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.READ,
                     StandardOpenOption.TRUNCATE_EXISTING);
             Arena arena = Arena.ofConfined()) {

            long fileSegmentSize = 0;
            long indexSize = 2 * Long.BYTES + (long) inMemoryStorage.size();
            for (Map.Entry<MemorySegment, Entry<MemorySegment>> e : inMemoryStorage.entrySet()) {
                fileSegmentSize += e.getKey().byteSize() + e.getValue().value().byteSize();
            }

            MemorySegment fileWriteSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    fileSegmentSize,
                    arena
            );

            MemorySegment indexSegment = indexChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    indexSize,
                    arena
            );

            int counter = 1;
            for (Map.Entry<MemorySegment, Entry<MemorySegment>> e : inMemoryStorage.entrySet()) {
                indexSegment.set(ValueLayout.JAVA_INT_UNALIGNED, indexOffset, counter);
                indexOffset += Integer.BYTES;
                indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, e.getKey().byteSize());
                indexOffset += Long.BYTES;
                indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, e.getValue().value().byteSize());
                indexOffset += Long.BYTES;
                indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, storageOffset);
                indexOffset += Long.BYTES;

                writeMemorySegment(fileWriteSegment, e.getValue().value(), storageOffset);
                storageOffset += e.getValue().value().byteSize();
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

    private long findEntryIndex(MemorySegment key) {
        long l = 0;
        long r = indexSegment.byteSize();

        while (r - l >= 28) {
            long m = (l + r) / 2;
            m -= m % 28;

            long keySize = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, m + Integer.BYTES);
            long offsetInFileSegment = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, m + Integer.BYTES + 2 * Long.BYTES);
            MemorySegment keyEntry = fileSegment.asSlice(offsetInFileSegment, keySize);

            if (MEMORY_SEGMENT_COMPARATOR.compare(keyEntry, key) < 0) {
                l = m;
            } else {
                r = m;
            }
        }
        return r;
    }
}
