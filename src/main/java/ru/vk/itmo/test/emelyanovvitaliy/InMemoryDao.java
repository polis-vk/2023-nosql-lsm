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
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    public static final String FILENAME = "sstable.save";
    private final Path sstablePath;
    private final Comparator<MemorySegment> comparator = (o1, o2) -> {
        if (o1 == o2) {
            return 0;
        }
        return o1.asByteBuffer().compareTo(o2.asByteBuffer());
    };
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> mappings = new ConcurrentSkipListMap<>(
            comparator
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
        for (Entry<MemorySegment> entry : mappings.values()) {
            size += entry.value().byteSize() + entry.key().byteSize() + 2 * Long.BYTES;
        }
        try (FileChannel fc = FileChannel.open(sstablePath, Set.of(CREATE, READ, WRITE));
             Arena arena = Arena.ofConfined()) {
            Collection<Entry<MemorySegment>> vals = mappings.values();
            MemorySegment mapped = fc.map(READ_WRITE, 0, size, arena);
            long offset = 0;
            for (Entry<MemorySegment> entry : vals) {
                mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
                offset += Long.BYTES;
                mapped.asSlice(offset, entry.key().byteSize()).copyFrom(entry.key());
                offset += entry.key().byteSize();
                mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                offset += Long.BYTES;
                mapped.asSlice(offset, entry.value().byteSize()).copyFrom(entry.value());
                offset += entry.value().byteSize();
            }
        }
    }

    private Entry<MemorySegment> getFromFile(MemorySegment key) {
        if (!Files.exists(sstablePath)) {
            return null;
        }
        String strKey = new String(key.toArray(ValueLayout.JAVA_BYTE), UTF_8);
        try (FileChannel fc = FileChannel.open(sstablePath, Set.of(CREATE, READ))) {
            MemorySegment mapped = fc.map(READ_ONLY, 0, fc.size(), Arena.ofAuto());
            long offset = 0;
            while (offset < fc.size()) {
                String parsedKey = null;
                long keySize = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                if (keySize == key.byteSize()) {
                    parsedKey = new String(mapped.asSlice(offset, keySize).toArray(ValueLayout.JAVA_BYTE), UTF_8);
                }
                offset += keySize;
                long valueSize = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                if (strKey.equals(parsedKey)) {
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
