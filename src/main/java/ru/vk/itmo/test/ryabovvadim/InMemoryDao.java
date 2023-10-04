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
import java.util.concurrent.*;

import static java.lang.foreign.ValueLayout.*;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> MEMORY_SEGMENT_COMPARATOR =
            (firstSegment, secondSegment) -> {
                if (firstSegment == null) {
                    return secondSegment == null ? 0 : -1;
                } else if (secondSegment == null) {
                    return 1;
                }

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
    private final Path ssTablePath;
    private final SSTable ssTable;

    public InMemoryDao() {
        this(null);
    }

    public InMemoryDao(Path sstablePath) {
        this.ssTablePath = sstablePath;
        if (sstablePath == null) {
            this.ssTable = null;
        } else {
            this.ssTable = new SSTable(arena, sstablePath);
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        maybeLoad(key);
        return storage.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        if (from == null) {
            return all();
        }
        maybeLoad(from, null);
        return storage.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        if (to == null) {
            return all();
        }
        maybeLoad(null, to);
        return storage.headMap(to).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        maybeLoad(null, null);
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
        maybeLoad(from, to);
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        arena.close();
        ssTable.close();
        save(new DataOutputStream(new FileOutputStream(ssTablePath.toString())));
    }

    public void maybeLoad(MemorySegment key) {
        if (ssTable == null || storage.containsKey(key)) {
            return;
        }

        Entry<MemorySegment> entry = ssTable.load(key);
        if (entry != null ) {
            storage.put(key, entry);
        }
    }

    public void maybeLoad(MemorySegment from, MemorySegment to) {
        if (ssTable == null || (storage.containsKey(from) && storage.containsKey(to))) {
            return;
        }

        ssTable.load(from, to).forEach(entry -> storage.put(entry.key(), entry));
    }

    public void save(DataOutput output) throws IOException {
        long offset = storage.size() * JAVA_BYTE.byteSize();
        List<Entry<MemorySegment>> segments = new ArrayList<>();

        output.writeInt(storage.size());
        for (Entry<MemorySegment> entry : storage.values()) {
            MemorySegment key = entry.key();
            MemorySegment value = entry.value();

            output.writeLong(offset);
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

    private static class SSTable implements Closeable {
        private final Map<MemorySegment, Integer> idsBySegment = new ConcurrentHashMap<>();
        private final Set<Integer> ids = ConcurrentHashMap.newKeySet();
        private final FileChannel fileChannel;
        private final MemorySegment mappedFile;

        public SSTable(Arena arena, Path file) {
            try {
                this.fileChannel = FileChannel.open(file);
                this.mappedFile = fileChannel.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        fileChannel.size(),
                        arena
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public Entry<MemorySegment> load(MemorySegment key) {
            int countRecords = mappedFile.get(JAVA_INT, 0);
            long offset = JAVA_INT.byteSize();

            for (int i = 0; i < countRecords; ++i) {
                long entryOffset = mappedFile.get(JAVA_LONG, offset);
                MemorySegment curKey = readKey(entryOffset);

                if (MEMORY_SEGMENT_COMPARATOR.compare(key, curKey) == 0) {
                    update(key, i);
                    return new BaseEntry<>(key, readValue(offset));
                }
                offset += JAVA_LONG.byteSize();
            }

            return null;
        }

        public List<Entry<MemorySegment>> load(MemorySegment from, MemorySegment to) {
            int countRecords = mappedFile.get(JAVA_INT, 0);

            List<Entry<MemorySegment>> loadedEntries = new ArrayList<>();
            boolean add = to == null;
            int fromIndex = idsBySegment.getOrDefault(from, 0);
            int toIndex = idsBySegment.getOrDefault(to, countRecords);
            for (int i = fromIndex; i < toIndex; ++i) {
                if (ids.contains(i)) {
                    continue;
                }

                long offset = JAVA_INT.byteSize() + i * JAVA_LONG.byteSize();
                long entryOffset = mappedFile.get(JAVA_LONG, offset);
                MemorySegment key = readKey(entryOffset);

                if (MEMORY_SEGMENT_COMPARATOR.compare(to, key) == 0) {
                    update(to, i);
                    break;
                }

                if (!add && MEMORY_SEGMENT_COMPARATOR.compare(from, key) == 0) {
                    update(from, i);
                    add = true;
                }

                if (add) {
                    loadedEntries.add(new BaseEntry<>(key, readValue(entryOffset)));
                }
            }

            return loadedEntries;
        }

        private MemorySegment readKey(long offset) {
            long keySize = mappedFile.get(JAVA_LONG, offset);
            offset += 2 * JAVA_LONG.byteSize();
            return mappedFile.asSlice(offset, keySize);
        }

        private MemorySegment readValue(long offset) {
            long keySize = mappedFile.get(JAVA_LONG, offset);
            offset += JAVA_LONG.byteSize();
            long valueSize = mappedFile.get(JAVA_LONG, offset);
            offset += JAVA_LONG.byteSize();

            return mappedFile.asSlice(offset + keySize, valueSize);
        }

        private void update(MemorySegment segment, int index) {
            idsBySegment.put(segment, index);
            ids.add(index);
        }

        @Override
        public void close() throws IOException {
            fileChannel.close();
        }
    }
}
