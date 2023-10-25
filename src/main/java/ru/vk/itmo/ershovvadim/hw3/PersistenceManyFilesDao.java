package ru.vk.itmo.ershovvadim.hw3;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

public class PersistenceManyFilesDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    static final String DATA = "data";
    static final String INDEX = "index";
    static final String SS_TABLE_DIR = "SSTable_";

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> db =
            new ConcurrentSkipListMap<>(Utils::compare);

    private final Path dir;
    private final Arena arena;
    private final List<SSTable> ssTables = new ArrayList<>();

    public PersistenceManyFilesDao(Config config) throws IOException {
        arena = Arena.ofShared();

        dir = config.basePath();
        if (!Files.exists(dir)) {
            return;
        }

        List<Path> ssTableDirs = getAllSSTableDir();

        for (var ssTableDir : ssTableDirs) {
            ssTables.add(new SSTable(ssTableDir, arena));
        }
    }

    private List<Path> getAllSSTableDir() throws IOException {
        List<Path> collect = new ArrayList<>();
        try (Stream<Path> fileStream = Files.list(dir)) {
            fileStream.forEach(path -> {
                if (path.getFileName().toString().startsWith(SS_TABLE_DIR)) {
                    collect.add(path);
                }
            });
        }
        return collect;
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> current = db.get(key);
        if (current != null) {
            return current.value() == null ? null : current;
        }

        Iterator<Entry<MemorySegment>> iterator = getMergeIterator(Collections.emptyIterator(), key, null);
        if (!iterator.hasNext()) {
            return null;
        }

        current = iterator.next();
        if (key.mismatch(current.key()) == -1) {
            return current;
        }
        return null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return getMergeIterator(getInMemory(from, to), from, to);
    }

    private MergeIterator getMergeIterator(
            Iterator<Entry<MemorySegment>> firstIter,
            MemorySegment from,
            MemorySegment to
    ) {
        List<PeekIterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        iterators.add(new PeekIteratorImpl(firstIter, Integer.MAX_VALUE));
        for (SSTable ssTable : ssTables) {
            iterators.add(ssTable.iterator(from, to));
        }
        return new MergeIterator(iterators);
    }

    public Iterator<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return this.db.values().iterator();
        } else if (to == null) {
            return this.db.tailMap(from).values().iterator();
        } else if (from == null) {
            return this.db.headMap(to).values().iterator();
        }
        return this.db.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        this.db.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (arena.scope().isAlive()) {
            arena.close();
        }

        if (db.isEmpty()) {
            return;
        }

        try (var writeArena = Arena.ofShared()) {
            long indexSize = indexSize();
            long tableSize = tableSize();
            Path ssTableDir = createDir();

            try (var fcTable = FileChannel.open(ssTableDir.resolve(DATA), TRUNCATE_EXISTING, CREATE, WRITE, READ);
                 var fcIndex = FileChannel.open(ssTableDir.resolve(INDEX), TRUNCATE_EXISTING, CREATE, WRITE, READ)
            ) {
                MemorySegment msTable = fcTable.map(FileChannel.MapMode.READ_WRITE, 0, tableSize, writeArena);
                MemorySegment msIndex = fcIndex.map(FileChannel.MapMode.READ_WRITE, 0, indexSize, writeArena);
                long indexOffset = 0;
                long tableOffset = 0;
                for (Entry<MemorySegment> entry : db.values()) {
                    msIndex.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tableOffset);
                    indexOffset += Long.BYTES;

                    tableOffset += writeInFile(entry.key(), msTable, tableOffset);
                    tableOffset += writeInFile(entry.value(), msTable, tableOffset);
                }
            }
        }
    }

    private Path createDir() throws IOException {
        Path ssTableDir = dir.resolve(SS_TABLE_DIR + ssTables.size());
        Files.createDirectories(ssTableDir);
        return ssTableDir;
    }

    private long tableSize() {
        return db.values()
                .stream()
                .map(entry -> {
                    long valueSize = (entry.value() == null) ? 0 : entry.value().byteSize();
                    return 2L * Long.BYTES + entry.key().byteSize() + valueSize;
                })
                .reduce(0L, Long::sum);
    }

    private long indexSize() {
        return (long) db.size() * Long.BYTES;
    }

    private long writeInFile(MemorySegment key, MemorySegment file, long fileOffset) {
        if (key == null) {
            file.set(ValueLayout.JAVA_LONG_UNALIGNED, fileOffset, -1);
            return Long.BYTES;
        }
        long keySize = key.byteSize();
        file.set(ValueLayout.JAVA_LONG_UNALIGNED, fileOffset, keySize);
        MemorySegment.copy(key, 0, file, fileOffset + Long.BYTES, keySize);
        return keySize + Long.BYTES;
    }

}
