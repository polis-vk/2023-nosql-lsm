package ru.vk.itmo.kononovvladimir;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemesDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = new MemoryComparator();
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private final Arena arena;
    private final DiskStorage diskStorage;
    private final Path path;
    private final Path indexFile;
    static final String FIRST_TABLE_NAME = "0.txt";
    static final String DIR_DATA = "data";
    static final String INDEX_FILE_NAME = "index.idx";

    public MemesDao(Config config) throws IOException {
        this.path = config.basePath().resolve(DIR_DATA);
        this.indexFile = path.resolve(INDEX_FILE_NAME);
        Files.createDirectories(path);

        this.arena = Arena.ofShared();

        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return diskStorage.range(getInMemory(from, to), from, to);
    }

    private Iterator<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.values().iterator();
        }
        if (from == null) {
            return storage.headMap(to).values().iterator();
        }
        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void compact() throws IOException {
        final Iterator<Entry<MemorySegment>> iterator = get(null, null);

        DiskStorage.save(path, () -> iterator, path.resolve(FIRST_TABLE_NAME));

        if (Files.exists(indexFile)) {
            List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
            //Вдруг неожиданно пропадет из-за внешних обстоятельств
            Files.deleteIfExists(indexFile);

            for (String fileName : existedFiles) {
                Files.deleteIfExists(path.resolve(fileName));
            }
        }
        storage.clear();

        Files.write(
                indexFile,
                List.of(FIRST_TABLE_NAME),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.get(key);
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        Iterator<Entry<MemorySegment>> iterator = diskStorage.range(Collections.emptyIterator(), key, null);

        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (comparator.compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        arena.close();

        if (!storage.isEmpty()) {
            DiskStorage.save(path, storage.values(), indexFile);
            //Вдруг вызовется потом повторно метод, а арена жива
            storage.clear();
        }
    }
}
