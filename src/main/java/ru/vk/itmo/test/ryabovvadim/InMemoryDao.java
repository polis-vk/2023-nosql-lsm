package ru.vk.itmo.test.ryabovvadim;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

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
                        firstSegment.get(JAVA_BYTE, mismatchOffset),
                        secondSegment.get(JAVA_BYTE, mismatchOffset)
                );
            };
    private static final String SSTABLE_FILE = "data.sst";

    private final Arena arena = Arena.ofShared();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);
    private final Config config;
    private SSTable ssTable;

    public InMemoryDao() {
        this(null);
    }

    public InMemoryDao(Config config) {
        this.config = config;
        if (config == null || config.basePath() == null) {
            this.ssTable = null;
        } else {
            try {
                this.ssTable = new SSTable(arena, config.basePath().resolve(SSTABLE_FILE));
            } catch (IOException e) {
                this.ssTable = null;
            }
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

        if (config != null) {
            save(config.basePath().resolve(SSTABLE_FILE));
        }
    }

    public void maybeLoad(MemorySegment key) {
        if (config == null || storage.containsKey(key)) {
            return;
        }

        Entry<MemorySegment> entry = ssTable.load(key);
        if (entry != null ) {
            storage.put(key, entry);
        }
    }

    public void maybeLoad(MemorySegment from, MemorySegment to) {
        if (config == null || (storage.containsKey(from) && storage.containsKey(to))) {
            return;
        }

        ssTable.load(from, to).forEach(entry -> storage.put(entry.key(), entry));
    }

    public void save(Path path) throws IOException {
        try (FileOutputStream outputFile = new FileOutputStream(path.toFile())) {
            DataOutput output = new DataOutputStream(outputFile);
            long offset = JAVA_INT.byteSize() + storage.size() * JAVA_BYTE.byteSize();
            Collection<Entry<MemorySegment>> segments = storage.values();

            output.writeInt(storage.size());
            for (Entry<MemorySegment> entry : segments) {
                MemorySegment key = entry.key();
                MemorySegment value = entry.value();

                output.writeLong(offset);
                offset += 2 * JAVA_LONG.byteSize() + key.byteSize() + value.byteSize();
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
    }

    private static class SSTable {
        private final Map<MemorySegment, Integer> idsBySegment = new ConcurrentHashMap<>();
        private final Set<Integer> ids = ConcurrentHashMap.newKeySet();
        private final MemorySegment mappedFile;
        private final int countRecords;
        private final List<Long> offsets = new ArrayList<>();

        public SSTable(Arena arena, Path file) throws IOException {
            try(FileChannel fileChannel = FileChannel.open(file)) {
                this.mappedFile = fileChannel.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        fileChannel.size(),
                        arena
                );

                this.countRecords = mappedFile.get(JAVA_INT, 0);
                long offset = JAVA_INT.byteSize();
                for (int i = 0; i < countRecords; ++i) {
                    offsets.add(mappedFile.get(JAVA_LONG, offset));
                    offset += JAVA_LONG.byteSize();
                }
            }
        }

        public Entry<MemorySegment> load(MemorySegment key) {
            for (int i = 0; i < countRecords; ++i) {
                Long entryOffset = offsets.get(i);
                MemorySegment curKey = readKey(entryOffset);

                if (MEMORY_SEGMENT_COMPARATOR.compare(key, curKey) == 0) {
                    update(key, i);
                    return new BaseEntry<>(key, readValue(entryOffset));
                }
            }

            return null;
        }

        public List<Entry<MemorySegment>> load(MemorySegment from, MemorySegment to) {
            List<Entry<MemorySegment>> loadedEntries = new ArrayList<>();
            boolean add = to == null;
            int fromIndex = idsBySegment.getOrDefault(from, 0);
            int toIndex = idsBySegment.getOrDefault(to, countRecords);

            for (int i = fromIndex; i < toIndex; ++i) {
                if (ids.contains(i)) {
                    continue;
                }

                Long entryOffset = offsets.get(i);
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
    }
}
