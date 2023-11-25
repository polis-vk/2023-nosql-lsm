package ru.vk.itmo.test.ryabovvadim;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.ryabovvadim.iterators.EntrySkipNullsIterator;
import ru.vk.itmo.test.ryabovvadim.iterators.FutureIterator;
import ru.vk.itmo.test.ryabovvadim.iterators.GatheringIterator;
import ru.vk.itmo.test.ryabovvadim.iterators.LazyIterator;
import ru.vk.itmo.test.ryabovvadim.iterators.PriorityIterator;
import ru.vk.itmo.test.ryabovvadim.utils.FileUtils;
import ru.vk.itmo.test.ryabovvadim.utils.MemorySegmentUtils;
import ru.vk.itmo.test.ryabovvadim.utils.NumberUtils;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static ru.vk.itmo.test.ryabovvadim.utils.FileUtils.DATA_FILE_EXT;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Arena arena = Arena.ofShared();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable =
            new ConcurrentSkipListMap<>(MemorySegmentUtils::compareMemorySegments);
    private final Config config;

    private final NavigableSet<SSTable> ssTables = new TreeSet<>(
            Comparator.comparingLong(SSTable::getId).reversed()
    );

    public InMemoryDao() throws IOException {
        this(null);
    }

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        if (!existsPath()) {
            return;
        }

        if (Files.notExists(config.basePath())) {
            Files.createDirectories(config.basePath());
        }
        updateSSTables(true);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> result = memoryTable.get(key);

        if (result == null) {
            result = load(key);
        }
        return handleDeletededEntry(result);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        if (from == null) {
            return all();
        }

        return makeIteratorWithSkipNulls(memoryTable.tailMap(from), load(from, null));
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        if (to == null) {
            return all();
        }

        return makeIteratorWithSkipNulls(memoryTable.headMap(to), load(null, to));
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return makeIteratorWithSkipNulls(memoryTable, load(null, null));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            return allTo(to);
        }
        if (to == null) {
            return allFrom(from);
        }

        return makeIteratorWithSkipNulls(memoryTable.subMap(from, to), load(from, to));
    }

    private Entry<MemorySegment> load(MemorySegment key) {
        if (!existsPath()) {
            return null;
        }

        for (SSTable ssTable : ssTables) {
            Entry<MemorySegment> entry = ssTable.findEntry(key);
            if (entry != null) {
                return handleDeletededEntry(entry);
            }
        }

        return null;
    }

    private List<FutureIterator<Entry<MemorySegment>>> load(MemorySegment from, MemorySegment to) {
        List<FutureIterator<Entry<MemorySegment>>> iterators = new ArrayList<>();

        for (SSTable ssTable : ssTables) {
            FutureIterator<Entry<MemorySegment>> iterator = ssTable.findEntries(from, to);
            if (iterator.hasNext()) {
                iterators.add(iterator);
            }
        }

        return iterators;
    }

    private Entry<MemorySegment> handleDeletededEntry(Entry<MemorySegment> entry) {
        if (entry == null || entry.value() == null) {
            return null;
        }
        return entry;
    }

    private FutureIterator<Entry<MemorySegment>> makeIteratorWithSkipNulls(
            Map<MemorySegment, Entry<MemorySegment>> memoryEntries,
            List<FutureIterator<Entry<MemorySegment>>> loadedIterators
    ) {
        Iterator<Entry<MemorySegment>> entriesIterator = memoryEntries.values().iterator();

        if (loadedIterators.isEmpty()) {
            return new EntrySkipNullsIterator(entriesIterator);
        }

        int priority = 0;
        List<PriorityIterator<Entry<MemorySegment>>> priorityIterators = new ArrayList<>();

        if (entriesIterator.hasNext()) {
            priorityIterators.add(new PriorityIterator<>(new LazyIterator<>(entriesIterator), priority++));
        }
        for (FutureIterator<Entry<MemorySegment>> it : loadedIterators) {
            priorityIterators.add(new PriorityIterator<>(it, priority++));
        }

        GatheringIterator<Entry<MemorySegment>> gatheringIterator = new GatheringIterator<>(
                priorityIterators,
                Comparator.comparing(
                        (PriorityIterator<Entry<MemorySegment>> it) -> it.showNext().key(),
                        MemorySegmentUtils::compareMemorySegments
                ).thenComparingInt(PriorityIterator::getPriority),
                Comparator.comparing(Entry::key, MemorySegmentUtils::compareMemorySegments)
        );

        return new EntrySkipNullsIterator(gatheringIterator);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memoryTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (existsPath() && !memoryTable.isEmpty()) {
            long ssTableId = saveEntries(memoryTable.values());
            memoryTable.clear();
            ssTables.add(new SSTable(config.basePath(), ssTableId, arena));
        }
    }

    @Override
    public void compact() throws IOException {
        if (existsPath()) {
            saveEntries(this::all);

            for (SSTable ssTable : ssTables) {
                ssTable.delete();
            }
            ssTables.clear();
            memoryTable.clear();

            updateSSTables(false);
        }
    }

    private void updateSSTables(boolean isStartup) throws IOException {
        Files.walkFileTree(config.basePath(), Set.of(), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (FileUtils.hasExtension(file, DATA_FILE_EXT) && NumberUtils.isInteger(
                        FileUtils.extractFileName(file, DATA_FILE_EXT)
                )) {
                    ssTables.add(new SSTable(
                            config.basePath(),
                            Long.parseLong(FileUtils.extractFileName(file, DATA_FILE_EXT)),
                            arena
                    ));
                    return FileVisitResult.CONTINUE;
                }

                if (isStartup) {
                    Files.deleteIfExists(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private long saveEntries(Iterable<Entry<MemorySegment>> entries) throws IOException {
        long maxTableNumber = 0;
        for (SSTable ssTable : ssTables) {
            maxTableNumber = Math.max(maxTableNumber, ssTable.getId());
        }
        SSTable.save(config.basePath(), maxTableNumber + 1, entries, arena);
        return maxTableNumber + 1;
    }

    private boolean existsPath() {
        return config != null && config.basePath() != null;
    }
}
