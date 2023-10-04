package ru.vk.itmo.test.ryabovvadim;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.foreign.ValueLayout.*;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> MEMORY_SEGMENT_COMPARATOR =
            (firstSegment, secondSegment) -> {
                long firstSegmentSize = firstSegment.byteSize();
                long secondSegmentSize = secondSegment.byteSize();
                long mismatchOffset = firstSegment.mismatch(secondSegment);

                if (mismatchOffset == firstSegmentSize) {
                    return -1;
                }
                if (mismatchOffset == secondSegmentSize) {
                    return 1;
                }
                if (mismatchOffset == -1) {
                    return Long.compare(firstSegmentSize, secondSegmentSize);
                }

                return Byte.compare(
                        firstSegment.get(ValueLayout.JAVA_BYTE, mismatchOffset),
                        secondSegment.get(ValueLayout.JAVA_BYTE, mismatchOffset)
                );
            };

    private final Arena arena = Arena.ofAuto();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);
    private final Path sstablePath;
    private final MemorySegment mappedSSTable;

    public InMemoryDao() {
        this(null);
    }

    public InMemoryDao(Path sstablePath) {
        this.sstablePath = sstablePath;
        if (sstablePath == null) {
            this.mappedSSTable = null;
        } else {
            try (FileChannel sstable = FileChannel.open(sstablePath)) {
                this.mappedSSTable = sstable.map(FileChannel.MapMode.READ_ONLY, 0, sstable.size(), arena);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        if (from == null) {
            return all();
        }
        return storage.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        if (to == null) {
            return all();
        }
        return storage.headMap(to).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return storage.values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            return allTo(to);
        }
        if (to == null) {
            return allFrom(from);
        }
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        save(new DataOutputStream(new FileOutputStream(sstablePath.toString())));
        arena.close();
    }

    public void maybe_load(MemorySegment key) {
        if (mappedSSTable == null) {
            return;
        }

        Entry<MemorySegment> entry = load(key);
        if (entry == null ) {
            return;
        }

        storage.put(entry.key(), entry);
    }

    public void save(DataOutput output) throws IOException {
        long i = 0;
        long offset = 2 * storage.size() * JAVA_BYTE.byteSize();
        List<Entry<MemorySegment>> segments = new ArrayList<>();

        output.writeInt(storage.size());
        for (Entry<MemorySegment> entry : storage.values()) {
            MemorySegment key = entry.key();
            MemorySegment value = entry.value();

            output.writeLong(i);
            output.writeLong(offset);
            ++i;
            offset += 2 * JAVA_LONG.byteSize() + key.byteSize() + value.byteSize();

            segments.add(entry);
        }

        for (var entry : segments) {
            MemorySegment key = entry.key();
            MemorySegment value = entry.value();

            output.writeLong(key.byteSize());
            output.writeLong(value.byteSize());
            output.write(key.toArray(JAVA_BYTE));
            output.write(value.toArray(JAVA_BYTE));
        }
    }

    public Entry<MemorySegment> load(MemorySegment key) {
        int countRecords = mappedSSTable.get(JAVA_INT, 0);
        long offset = JAVA_INT.byteSize();

        for (int i = 0; i < countRecords; ++i) {
            offset += JAVA_LONG.byteSize();
            long entryOffset = mappedSSTable.get(JAVA_LONG, offset);
            MemorySegment curKey = readKey(mappedSSTable, entryOffset);

            if (MEMORY_SEGMENT_COMPARATOR.compare(key, curKey) == 0) {
                return new BaseEntry<>(curKey, readValue(mappedSSTable, offset));
            }
        }

        return null;
    }

    public static MemorySegment readKey(MemorySegment segment, long offset) {
        long keySize = segment.get(JAVA_LONG, offset);
        offset += 2 * JAVA_LONG.byteSize();
        return segment.asSlice(offset, keySize);
    }

    public static MemorySegment readValue(MemorySegment segment, long offset) {
        long keySize = segment.get(JAVA_LONG, offset);
        offset += JAVA_LONG.byteSize();
        long valueSize = segment.get(JAVA_LONG, offset);
        offset += JAVA_LONG.byteSize();

        return segment.asSlice(offset + keySize, valueSize);
    }
}
