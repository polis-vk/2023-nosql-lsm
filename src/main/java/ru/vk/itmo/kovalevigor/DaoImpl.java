package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage;
    private final Config config;
    private final long ENTRY_META_SIZE = ValueLayout.JAVA_LONG.byteSize() * 2;
    public final String SSTABLE_NAME = "sstable";

    public static final Comparator<MemorySegment> COMPARATOR = (lhs, rhs) -> {
        final long mismatch = lhs.mismatch(rhs);
        final long lhsSize = lhs.byteSize();
        final long rhsSize = rhs.byteSize();
        final long minSize = Math.min(lhsSize, rhsSize);
        if (mismatch == -1) {
            return 0;
        } else if (minSize == mismatch) {
            return Long.compare(lhsSize, rhsSize);
        }
        return Byte.compare(getByte(lhs, mismatch), getByte(rhs, mismatch));
    };

    public DaoImpl() {
        storage = new ConcurrentSkipListMap<>(COMPARATOR);
        this.config = null;
    }

    public DaoImpl(Config config) throws IOException {
        storage = new ConcurrentSkipListMap<>(COMPARATOR);
        this.config = config;
        readMap();
    }

    private Path getSSTablePath() {
        return this.config.basePath().resolve(SSTABLE_NAME);
    }

    private void readMap() throws IOException {
        if (this.config == null) {
            return;
        }
        final Path path = getSSTablePath();
        if (Files.notExists(path)) {
            return;
        }

        try (FileChannel writer = FileChannel.open(
                getSSTablePath(),
                StandardOpenOption.READ)
        ) {

            long offset = 0;
            final MemorySegment memorySegment = writer.map(
                    FileChannel.MapMode.READ_ONLY,
                    offset,
                    Files.size(path),
                    Arena.ofConfined()
            );

            while (offset < memorySegment.byteSize()) {
                long key_size = memorySegment.asSlice(offset, ValueLayout.JAVA_LONG.byteSize())
                        .get(ValueLayout.JAVA_LONG, 0);
                offset += ValueLayout.JAVA_LONG.byteSize();
                MemorySegment key = memorySegment.asSlice(offset, key_size);
                offset += key_size + getAlignment(key_size, ValueLayout.JAVA_LONG);

                long value_size = memorySegment.asSlice(offset, ValueLayout.JAVA_LONG.byteSize())
                        .get(ValueLayout.JAVA_LONG, 0);
                offset += ValueLayout.JAVA_LONG.byteSize();
                MemorySegment value = memorySegment.asSlice(offset, value_size);
                offset += value_size + getAlignment(value_size, ValueLayout.JAVA_LONG);
                upsert(new BaseEntry<>(key, value));
            }
        }
        System.out.println(storage.size());
    }

    private static <T> Iterator<T> getValuesIterator(final ConcurrentNavigableMap<?, T> map) {
        return map.values().iterator();
    }

    private static byte getByte(final MemorySegment memorySegment, final long offset) {
        return memorySegment.get(ValueLayout.JAVA_BYTE, offset);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        if (from == null) {
            if (to == null) {
                return all();
            }
            return allTo(to);
        } else if (to == null) {
            return allFrom(from);
        }
        return getValuesIterator(storage.subMap(from, to));
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        Objects.requireNonNull(entry);
        storage.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(final MemorySegment from) {
        Objects.requireNonNull(from);
        return getValuesIterator(storage.tailMap(from));
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(final MemorySegment to) {
        Objects.requireNonNull(to);
        return getValuesIterator(storage.headMap(to));
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return getValuesIterator(storage);
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        Objects.requireNonNull(key);
        return storage.get(key);
    }

    private static long getAlignment(final long size, final ValueLayout layout) {
        final long layoutSize = layout.byteSize();
        return (layoutSize - size % layoutSize) % layoutSize;
    }

    private static long getAlignedSize(final long size, final ValueLayout layout) {
        return size + getAlignment(size, layout);
    }

    private long getTotalMapSize() {
        long total_size = ENTRY_META_SIZE * storage.size();
        for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : storage.entrySet()) {
            total_size += getAlignedSize(entry.getKey().byteSize(), ValueLayout.JAVA_LONG)
                    + getAlignedSize(entry.getValue().value().byteSize(), ValueLayout.JAVA_LONG);
        }
        return total_size;
    }

    private static long putMemorySegment(
            final MemorySegment writable,
            long offset,
            final MemorySegment memorySegment
    ) {
        final long memorySegmentSize = memorySegment.byteSize();
        final long longSize = ValueLayout.JAVA_LONG.byteSize();
        writable.set(ValueLayout.JAVA_LONG, offset, memorySegmentSize);
        offset += longSize;
//        System.out.println(offset);
        writable.asSlice(offset).copyFrom(memorySegment);
        return offset + getAlignedSize(memorySegmentSize, ValueLayout.JAVA_LONG);
    }

    private static long putEntry(
            final MemorySegment writable,
            long offset,
            Map.Entry<MemorySegment, Entry<MemorySegment>> entry
    ) {
        offset = putMemorySegment(writable, offset, entry.getKey());
        return putMemorySegment(writable, offset, entry.getValue().value());
    }

    @Override
    public void close() throws IOException {
        if (this.config == null) {
            return;
        }
        final Path path = this.config.basePath().resolve(SSTABLE_NAME);
        try (FileChannel writer = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)
        ) {

            long offset = 0;
            final MemorySegment memorySegment = writer.map(
                    FileChannel.MapMode.READ_WRITE,
                    offset,
                    getTotalMapSize(),
                    Arena.ofConfined()
            );
//            memorySegment.set(ValueLayout.JAVA_LONG, 8, 100);

            for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : storage.entrySet()) {
                offset = putEntry(memorySegment, offset, entry);
            }
        }
    }
}
