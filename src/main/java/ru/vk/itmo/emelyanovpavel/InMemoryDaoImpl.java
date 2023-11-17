package ru.vk.itmo.emelyanovpavel;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    public static final long NULL_VALUE = -1;
    private static final String DIR_MAME = "ss_table";
    private static final String DATA_NAME = "data.txt";
    private static final String INDEX_NAME = "index.txt";
    private static final Set<OpenOption> WRITE_OPTIONS = Set.of(
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
    );
    private final Path configPath;
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage;
    private final List<SSTable> ssTables = new ArrayList<>();
    private final Arena arena = Arena.ofShared();

    public InMemoryDaoImpl(Path path) throws IOException {
        configPath = path;
        storage = new ConcurrentSkipListMap<>(new MemorySegmentComparator());

        if (!Files.exists(configPath)) {
            return;
        }

        ssTables.addAll(getSSTablesFromMemory(path));
    }

    public InMemoryDaoImpl() {
        configPath = null;
        storage = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (configPath == null) {
            return storage.get(key);
        }
        Iterator<Entry<MemorySegment>> it = get(key, null);
        if (it.hasNext()) {
            Entry<MemorySegment> result = it.next();
            if (key.mismatch(result.key()) == -1) {
                return result;
            }
        }
        return null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> inMemoryIterator = getInMemory(from, to);
        if (configPath == null) {
            return inMemoryIterator;
        }
        List<PeekIterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        iterators.add(new PeekIteratorImpl(inMemoryIterator, Integer.MAX_VALUE));
        iterators.addAll(getIteratorsFromTables(from, to));
        return new MergeIterator(iterators);
    }

    public Iterator<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return all();
        }
        if (from == null) {
            return allTo(to);
        }
        if (to == null) {
            return allFrom(from);
        }
        return storage.subMap(from, to)
                .values()
                .iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return storage.tailMap(from)
                .values()
                .iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return storage.headMap(to)
                .values()
                .iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return storage.values()
                .iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        if (arena.scope().isAlive()) {
            arena.close();
        }
        flush();
    }

    @Override
    public void flush() throws IOException {
        if (storage.isEmpty()) {
            return;
        }
        long indexSize = (long) storage.size() * Long.BYTES;

        long storageSize = calculateCurrentStorageSize();

        Path ssTableDir = configPath.resolve(DIR_MAME + ssTables.size());
        Files.createDirectories(ssTableDir);

        try (var fcTable = FileChannel.open(ssTableDir.resolve(DATA_NAME), WRITE_OPTIONS)) {
            try (var fcIndex = FileChannel.open(ssTableDir.resolve(INDEX_NAME), WRITE_OPTIONS)) {
                try (Arena arenaForWriting = Arena.ofShared()) {
                    MemorySegment msData = fcTable.map(READ_WRITE, 0, storageSize, arenaForWriting);
                    MemorySegment msIndex = fcIndex.map(READ_WRITE, 0, indexSize, arenaForWriting);
                    long indexOffset = 0;
                    long dataOffset = 0;
                    for (Entry<MemorySegment> entry : storage.values()) {
                        msIndex.set(JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                        indexOffset += Long.BYTES;

                        writeDataToMemorySegment(entry.key(), msData, dataOffset);
                        dataOffset = getNextOffsetAfterInsertion(entry.key(), dataOffset);

                        writeDataToMemorySegment(entry.value(), msData, dataOffset);
                        dataOffset = getNextOffsetAfterInsertion(entry.value(), dataOffset);
                    }
                }
            }
        }
    }

    private long calculateCurrentStorageSize() {
        return getAmountOfBytesToStoreKeyAndValue() + getAmountOfBytesToStoreKeyAndValueSize();
    }

    private long getAmountOfBytesToStoreKeyAndValueSize() {
        return 2L * storage.size() * Long.BYTES;
    }

    private long getAmountOfBytesToStoreKeyAndValue() {
        return storage.values()
                .stream()
                .mapToLong(entry -> {
                    long valueSize = (entry.value() == null) ? 0 : entry.value().byteSize();
                    return valueSize + entry.key().byteSize();
                })
                .sum();
    }

    private void writeDataToMemorySegment(MemorySegment dataToInsert, MemorySegment ms, long currentOffset) {
        if (dataToInsert == null) {
            ms.set(JAVA_LONG_UNALIGNED, currentOffset, NULL_VALUE);
            return;
        }
        long dataSize = dataToInsert.byteSize();
        ms.set(JAVA_LONG_UNALIGNED, currentOffset, dataSize);
        MemorySegment.copy(dataToInsert, 0, ms, currentOffset + Long.BYTES, dataSize);
    }

    private long getNextOffsetAfterInsertion(MemorySegment dataToInsert, long currentOffset) {
        long result = currentOffset + Long.BYTES;
        if (dataToInsert == null) {
            return result;
        }
        return result + dataToInsert.byteSize();
    }

    private List<SSTable> getSSTablesFromMemory(Path path) throws IOException {
        List<SSTable> result = new ArrayList<>();
        Files.walkFileTree(
                path,
                Set.of(),
                2,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        String currentDirName = dir.getFileName().toString();
                        if (currentDirName.startsWith(DIR_MAME)) {
                            int priority = Integer.parseInt(currentDirName.substring(DIR_MAME.length()));

                            Path dataFile = dir.resolve(DATA_NAME);
                            Path indexFile = dir.resolve(INDEX_NAME);
                            try (FileChannel dataChanel = FileChannel.open(dataFile, StandardOpenOption.READ)) {
                                try (FileChannel indexChanel = FileChannel.open(indexFile, StandardOpenOption.READ)) {
                                    MemorySegment msData = dataChanel.map(READ_ONLY, 0, Files.size(dataFile), arena);
                                    MemorySegment msIndex = indexChanel.map(READ_ONLY, 0, Files.size(indexFile), arena);
                                    result.add(new SSTable(msData, msIndex, priority));
                                }
                            }
                        }
                        return super.postVisitDirectory(dir, exc);
                    }
                }
        );
        return result;
    }

    private List<PeekIterator<Entry<MemorySegment>>> getIteratorsFromTables(MemorySegment from, MemorySegment to) {
        return ssTables.stream()
                .map(table -> table.iterator(from, to))
                .toList();
    }
}
