package ru.vk.itmo.solnyshkoksenia;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.solnyshkoksenia.MemorySegmentComparator;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.IntStream;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> comparator = new MemorySegmentComparator();
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private Config config;
    private List<Arena> arenas;

    public DaoImpl() {
        // Empty constructor
    }

    public DaoImpl(Config config) {
        this.config = config;
        int SSTablesLen = getSSTables().length;
        arenas = IntStream.range(0, SSTablesLen).mapToObj(arena -> Arena.ofShared()).toList();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        NavigableMap<MemorySegment, Entry<MemorySegment>> range = new ConcurrentSkipListMap<>(comparator);

        if (from == null && to == null) {
            range.putAll(storage);
        } else if (from == null) {
            range.putAll(storage.headMap(to));
        } else if (to == null) {
            range.putAll(storage.tailMap(from));
        } else {
            range.putAll(storage.subMap(from, to));
        }

        File[] SSTables = getSSTables();

        for (File SSTable : SSTables) {
            MemorySegment mappedSSTable = Objects.requireNonNull(mapSSTable(SSTable));

            long SSTableSize = mappedSSTable.get(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, 0);
            long fromIndex, toIndex;

            if (from == null && to == null) {
                fromIndex = 0;
                toIndex = SSTableSize;
            } else if (from == null) {
                fromIndex = 0;
                toIndex = binarySearch(mappedSSTable, to);
                toIndex = toIndex < 0 ? -toIndex - 1 : toIndex;
            } else if (to == null) {
                fromIndex = binarySearch(mappedSSTable, from);
                fromIndex = fromIndex < 0 ? -fromIndex - 1 : fromIndex;
                toIndex = SSTableSize;
            } else {
                fromIndex = binarySearch(mappedSSTable, from);
                fromIndex = fromIndex < 0 ? -fromIndex - 1 : fromIndex;
                toIndex = binarySearch(mappedSSTable, to);
                toIndex = toIndex < 0 ? -toIndex - 1 : toIndex;
            }

            for (long i = fromIndex; i < toIndex; i++) {
                long keyPointer = mappedSSTable.get(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, i * 4 * Long.BYTES + Long.BYTES);
                long keySize = mappedSSTable.get(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, i * 4 * Long.BYTES + 2 * Long.BYTES);
                long valuePointer = mappedSSTable.get(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, i * 4 * Long.BYTES + 3 * Long.BYTES);
                long valueSize = mappedSSTable.get(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, i * 4 * Long.BYTES + 4 * Long.BYTES);
                MemorySegment key, value;
                key = mappedSSTable.asSlice(keyPointer, keySize);
                if (valuePointer == -1) {
                    value = null;
                } else {
                    value = mappedSSTable.asSlice(valuePointer, valueSize);
                }
                range.putIfAbsent(key, new BaseEntry<>(key, value));
            }
        }

        range.values().removeIf(entry -> entry.value() == null);
        return range.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.get(key);
        if (entry != null) {
            return entry.value() == null ? null : entry;
        }

        File[] SSTables = getSSTables();

        for (File SSTable : SSTables) {
            MemorySegment mappedSSTable = Objects.requireNonNull(mapSSTable(SSTable));

            long index = binarySearch(mappedSSTable, key);
            if (index >= 0) {
                long valuePointer = mappedSSTable.get(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, index * 4 * Long.BYTES + 3 * Long.BYTES);
                if (valuePointer == -1) {
                    return null;
                } else {
                    long valueSize = mappedSSTable.get(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, index * 4 * Long.BYTES + 4 * Long.BYTES);
                    MemorySegment value = mappedSSTable.asSlice(valuePointer, valueSize);
                    return new BaseEntry<>(key, value);
                }
            }
        }
        return null;
    }

    private long binarySearch(MemorySegment mappedSSTable, MemorySegment key) {
        long SSTableSize = mappedSSTable.get(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, 0);
        long low = 0;
        long high = SSTableSize - 1;

        while (low <= high) {
            long mid = (low + high) >>> 1;
            long midKeyPointer = mappedSSTable.get(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, mid * 4 * Long.BYTES + Long.BYTES);
            long midKeySize = mappedSSTable.get(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, mid * 4 * Long.BYTES + 2 * Long.BYTES);
            MemorySegment midVal = mappedSSTable.asSlice(midKeyPointer, midKeySize);
            int cmp = comparator.compare(midVal, key);

            if (cmp < 0) {
                low = mid + 1;
            }
            else if (cmp > 0) {
                high = mid - 1;
            }
            else {
                return mid;
            }
        }
        return -(low + 1);
    }

    private MemorySegment mapSSTable(File SSTable) {
        try (FileChannel fileChannel = FileChannel.open(SSTable.toPath(), StandardOpenOption.READ)) {
            return fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(SSTable.toPath()), arenas.get(getIndex(SSTable) - 1));
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        arenas.forEach(Arena::close);

        int index = Objects.requireNonNull(config.basePath().toFile().listFiles()).length + 1;

        long storageSize = Long.BYTES + 4L * storage.size() * Long.BYTES;
        for (Entry<MemorySegment> entry : storage.values()) {
            long valueSize = entry.value() == null ? 0 : entry.value().byteSize();
            storageSize += entry.key().byteSize() + valueSize;
        }

        try (Arena writeArena = Arena.ofConfined()) {
            try (FileChannel fileChannel = FileChannel.open(getPathToSSTable(index), StandardOpenOption.READ, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
                MemorySegment mappedSSTable = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, storageSize, writeArena);

                long offset = 0;
                mappedSSTable.set(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, offset, storage.size());
                offset += Long.BYTES;
                long offsetCounter = Long.BYTES + 4L * storage.size() * Long.BYTES;

                for (Entry<MemorySegment> entry : storage.values()) {
                    mappedSSTable.set(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, offset, offsetCounter);
                    offset += Long.BYTES;
                    mappedSSTable.set(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
                    offsetCounter += entry.key().byteSize();
                    offset += Long.BYTES;

                    if (entry.value() == null) {
                        mappedSSTable.set(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, offset, -1);
                        offset += Long.BYTES;
                        mappedSSTable.set(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, offset, -1);
                    } else {
                        mappedSSTable.set(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, offset, offsetCounter);
                        offset += Long.BYTES;
                        mappedSSTable.set(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                        offsetCounter += entry.value().byteSize();
                    }
                    offset += Long.BYTES;
                }

                for (Entry<MemorySegment> entry : storage.values()) {
                    MemorySegment.copy(entry.key(), 0, mappedSSTable, offset, entry.key().byteSize());
                    offset += entry.key().byteSize();

                    if (entry.value() != null) {
                        MemorySegment.copy(entry.value(), 0, mappedSSTable, offset, entry.value().byteSize());
                        offset += entry.value().byteSize();
                    }
                }

            }
        }
    }



    @SuppressWarnings("StringSplitter")
    private int getIndex(File SSTable) {
        return Integer.parseInt(SSTable.getName().split("SSTable")[1]);
//        return Integer.parseInt(Iterables.get(Splitter.on("SSTable").split(SSTable.getName()), 1));
    }

    private File[] getSSTables() {
        if (!Files.isDirectory(config.basePath())) {
            return new File[0];
        }
        File[] SSTables = config.basePath().toFile().listFiles();
        int countSSTables = Objects.requireNonNull(SSTables).length;
        File[] sortedSSTables = new File[countSSTables];
        for (File SSTable : SSTables) {
            if (!SSTable.getName().contains("Binary")) {
                int i = getIndex(SSTable);
                sortedSSTables[countSSTables - i] = SSTable;
            }
        }
        return sortedSSTables;
    }

    private Path getPathToSSTable(int index) {
        return config.basePath().resolve("SSTable" + index);
    }
}
