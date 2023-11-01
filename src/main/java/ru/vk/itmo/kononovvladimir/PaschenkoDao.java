package ru.vk.itmo.kononovvladimir;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class PaschenkoDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = PaschenkoDao::compare;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private final Arena arena;
    private final DiskStorage diskStorage;
    private final Path path;

    public PaschenkoDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);

        arena = Arena.ofShared();

        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena));
    }

    static int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        long mismatch = memorySegment1.mismatch(memorySegment2);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == memorySegment1.byteSize()) {
            return -1;
        }

        if (mismatch == memorySegment2.byteSize()) {
            return 1;
        }
        byte b1 = memorySegment1.get(ValueLayout.JAVA_BYTE, mismatch);
        byte b2 = memorySegment2.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
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
        Iterator<Entry<MemorySegment>> iterator = get(null, null);
/*        long size = 0;
        int count = 0;
        while (iterator.hasNext()) {
            Entry<MemorySegment> entry = iterator.next();
            size += entry.key().byteSize() + entry.value().byteSize() + Long.BYTES * 2;
            count++;
        }*/
        final Path indexFile = path.resolve("index.idx");
        final Path indexTmp = path.resolve("index.tmp");
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        Iterator<Entry<MemorySegment>> finalIterator = iterator;

        for (String file: existedFiles){
            Files.deleteIfExists(path.resolve(file));
        }
        Files.deleteIfExists(indexFile);
        Files.createFile(indexTmp);
        DiskStorage.save(path, () -> finalIterator);
        storage.clear();


/*        try (
                FileChannel fileChannel = FileChannel.open(
                        path.resolve("dataTmp"),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    size,
                    writeArena
            );

            long offsetKeys = 0;
            long offsetData = 2L * Long.BYTES * count;
            while (iterator.hasNext()) {
                Entry<MemorySegment> entry = iterator.next();
                MemorySegment.copy(entry.key(), 0, fileSegment, offsetKeys, entry.key().byteSize());
                MemorySegment.copy(entry.value(), 0, fileSegment, offsetData, entry.value().byteSize());
                offsetKeys += entry.key().byteSize();
                offsetData += entry.value().byteSize();
            }

            String newFileName = String.valueOf(0);

            Files.write(
                    path.resolve("index.idx"),
                    List.of(newFileName),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
        }*/
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
        if (compare(next.key(), key) == 0) {
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
            DiskStorage.save(path, storage.values());
        }
    }
}
