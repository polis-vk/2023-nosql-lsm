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
import java.time.Instant;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    public static final String FILENAME = "sstable.save";
    private final Path sstablePath;
    private final Arena arena = Arena.ofShared();
    public static final Comparator<MemorySegment> comparator = (o1, o2) -> {
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
        return getFromFile(key, sstablePath);
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
        dumpToFile(sstablePath);
    }

    @Override
    public void close() throws IOException {
        try {
            flush();
        } finally {
            if (arena.scope().isAlive()) {
                arena.close();
            }
        }
    }

    private void dumpToFile(Path path) throws IOException {
        long size = 0;
        Set<StandardOpenOption> openOptions =
                Set.of(
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING
                );
        for (Entry<MemorySegment> entry : mappings.values()) {
            size += entry.value().byteSize() + entry.key().byteSize();
        }
        size += Integer.BYTES + (2L * Long.BYTES + 1) * mappings.size();
        try (FileChannel fc = FileChannel.open(path, openOptions); Arena writeArena = Arena.ofConfined()) {
            MemorySegment mapped = fc.map(READ_WRITE, 0, size, writeArena);
            long offset = 0;
            long offsetToWrite = 0;
            mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, Instant.now().toEpochMilli());
            offset += Long.BYTES;
            mapped.set(ValueLayout.JAVA_INT_UNALIGNED, offset, mappings.size());
            offset += Integer.BYTES;
            offsetToWrite += Integer.BYTES + (2L * mappings.size() + 1) * Long.BYTES;
            for (Entry<MemorySegment> entry: mappings.values()) {
                mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, offsetToWrite);
                offset += Long.BYTES;
                MemorySegment.copy(
                        entry.key(), 0,
                        mapped, offsetToWrite,
                        entry.key().byteSize()
                );
                offsetToWrite += entry.key().byteSize();
                if (entry.value() == null) {
                    mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, -1);
                } else {
                    mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, offsetToWrite);
                    MemorySegment.copy(
                            entry.value(), 0,
                            mapped, offsetToWrite,
                            entry.value().byteSize()
                    );
                    offsetToWrite += entry.value().byteSize();
                }
                offset += Long.BYTES;
            }
        }
    }

    private Entry<MemorySegment> getFromFile(MemorySegment key, Path filePath) {
        if (!Files.exists(filePath)) {
            return null;
        }
        Set<StandardOpenOption> openOptions = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ);
        try (FileChannel fc = FileChannel.open(filePath, openOptions)) {
            MemorySegment mapped = fc.map(READ_ONLY, 0, fc.size(), arena);
            int numOfKeys = mapped.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
            long firstKeyOffset = Integer.BYTES + Long.BYTES + 2L * numOfKeys * Long.BYTES;
            int left = 0;
            int right = numOfKeys;
            while (left < right) {
                int m = (left + right) / 2;
                long keyAddressOffset = Integer.BYTES + (2L * m + 1) * Long.BYTES;
                long valueAddressOffset = keyAddressOffset + Long.BYTES;
                long keyOffset = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, keyAddressOffset);
                long valueOffset = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, valueAddressOffset);
                long keyOffsetTo;
                long nextKeyAddressOffset = keyAddressOffset + 2L * Long.BYTES;
                long nextKeyOffset = mapped.byteSize();
                if (valueOffset != -1) {
                    keyOffsetTo = valueOffset;
                } else if (nextKeyAddressOffset >= firstKeyOffset) {
                    keyOffsetTo = mapped.byteSize();
                } else {
                    keyOffsetTo = nextKeyAddressOffset - keyAddressOffset;
                }
                if (nextKeyAddressOffset < firstKeyOffset) {
                    nextKeyOffset = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, nextKeyAddressOffset);
                }
                long compared = comparator.compare(
                        key,
                        mapped.asSlice(keyOffset, keyOffsetTo - keyOffset)
                );

                if (compared == 0) {
                    if (valueOffset != -1) {

                        return new BaseEntry<>(
                                key,
                                mapped.asSlice(valueOffset, nextKeyOffset - valueOffset)
                        );
                    } else {
                        return new BaseEntry<>(
                                key,
                                null
                        );
                    }
                } else if (compared > 0) {
                    left = m + 1;
                } else {
                    right = m;
                }
            }
            return null;
        } catch (IOException ignored) {
            return null;
        }
    }
}
