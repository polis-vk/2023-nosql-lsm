package ru.vk.itmo.test.emelyanovvitaliy;

import ru.vk.itmo.BaseEntry;
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
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    public static final String FILENAME = "sstable.save";
    private final Path sstablePath;
    private final Arena arena = Arena.ofShared();
    private final Comparator<MemorySegment> comparator = (o1, o2) -> {
        if (o1 == o2) {
            return 0;
        }
        return o1.asByteBuffer().compareTo(o2.asByteBuffer());
    };
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> mappings = new ConcurrentSkipListMap<>(
            new MemSegmentComparator()
    );

    public InMemoryDao() {
        sstablePath = null;
    }

    public InMemoryDao(Path basePath) {
        sstablePath = basePath.resolve(FILENAME);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> memRes = mappings.getOrDefault(key, null);
        if (memRes != null) {
            return memRes;
        }
        return getFromFile(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return mappings.values().iterator();
        } else if (from == null) {
            return mappings.headMap(to).values().iterator();
        } else if (to == null) {
            return mappings.headMap(from).values().iterator();
        }
        return mappings.subMap(from, to)
                .sequencedValues().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        mappings.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        long size = 0;
        Set<StandardOpenOption> openOptions =
                Set.of(
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING
                );
        for (Entry<MemorySegment> entry : mappings.values()) {
            size += entry.value().byteSize() + entry.key().byteSize() + 2 * Long.BYTES;
        }
        try (FileChannel fc = FileChannel.open(sstablePath, openOptions); Arena writeArena = Arena.ofConfined()) {
            Collection<Entry<MemorySegment>> vals = mappings.values();
            MemorySegment mapped = fc.map(READ_WRITE, 0, size, writeArena);
            long offset = 0;
            for (Entry<MemorySegment> entry : vals) {
                mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
                offset += Long.BYTES;
                mapped.asSlice(offset, entry.key().byteSize()).copyFrom(entry.key());
                MemorySegment.copy(
                        entry.key(), 0,
                        mapped, offset,
                        entry.key().byteSize()
                );
                offset += entry.key().byteSize();
                mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                offset += Long.BYTES;
                MemorySegment.copy(
                        entry.value(), 0,
                        mapped, offset,
                        entry.value().byteSize()
                );
                offset += entry.value().byteSize();
            }
        } finally {
            if (arena.scope().isAlive()) {
                arena.close();
            }
        }
    }

    private Entry<MemorySegment> getFromFile(MemorySegment key) {
        if (!Files.exists(sstablePath)) {
            return null;
        }
        Set<StandardOpenOption> openOptions = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ);
        try (FileChannel fc = FileChannel.open(sstablePath, openOptions)) {
            MemorySegment mapped = fc.map(READ_ONLY, 0, fc.size(), arena);
            long offset = 0;
            while (offset < fc.size()) {
                long keySize = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                long mismatch = 0;
                if (keySize == key.byteSize()) {
                    mismatch = MemorySegment.mismatch(mapped, offset, offset + keySize, key, 0, keySize);
                }
                offset += keySize;
                long valueSize = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                if (mismatch == -1) {
                    return new BaseEntry<>(
                            key, mapped.asSlice(offset, valueSize)
                    );
                } else {
                    offset += valueSize;
                }
            }
        } catch (IOException ignored) {
            return null;
        }
        return null;
    }
}
