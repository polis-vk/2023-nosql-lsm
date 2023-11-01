package ru.vk.itmo.test.ryabovvadim;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.ryabovvadim.iterators.FutureIterator;
import ru.vk.itmo.test.ryabovvadim.iterators.GatheringIterator;
import ru.vk.itmo.test.ryabovvadim.iterators.LazyIterator;
import ru.vk.itmo.test.ryabovvadim.iterators.PriorityIterator;
import ru.vk.itmo.test.ryabovvadim.utils.FileUtils;
import ru.vk.itmo.test.ryabovvadim.utils.MemorySegmentUtils;

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
    private final List<SSTable> ssTables = new ArrayList<>();

    public InMemoryDao() throws IOException {
        this(null);
    }

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        if (!existsPath()) {
            return;
        }

        if (Files.notExists(config.basePath())) {
            Files.createDirectory(config.basePath());
        }

        NavigableSet<Long> dataFileNumbers = new TreeSet<>(Comparator.reverseOrder());
        Files.walkFileTree(config.basePath(), Set.of(), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (file.getFileName().toString().endsWith("." + DATA_FILE_EXT)) {
                    dataFileNumbers.add(Long.parseLong(
                            file.getFileName().toString().substring(
                                    0,
                                    file.getFileName().toString().indexOf("." + DATA_FILE_EXT)
                            )
                    ));
                }

                return FileVisitResult.CONTINUE;
            }
        });

        for (long number : dataFileNumbers) {
            ssTables.add(new SSTable(config.basePath(), Long.toString(number), arena));
        }
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

        return makeIteratorWithSkipNulls(memoryTable.tailMap(from), from, null);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        if (to == null) {
            return all();
        }

        return makeIteratorWithSkipNulls(memoryTable.headMap(to), null, to);
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return makeIteratorWithSkipNulls(memoryTable, null, null);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            return allTo(to);
        }
        if (to == null) {
            return allFrom(from);
        }

        return makeIteratorWithSkipNulls(memoryTable.subMap(from, to), from, to);
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
            Map<MemorySegment, Entry<MemorySegment>> entries,
            MemorySegment from,
            MemorySegment to
    ) {
        List<FutureIterator<Entry<MemorySegment>>> loadedIterators = load(from, to);
        Iterator<Entry<MemorySegment>> entriesIterator = entries.values().iterator();

        int priority = 0;
        List<PriorityIterator<Entry<MemorySegment>>> priorityIterators = new ArrayList<>();

        if (entriesIterator.hasNext()) {
            priorityIterators.add(new PriorityIterator<>(
                    new LazyIterator<>(entriesIterator::next, entriesIterator::hasNext),
                    priority
            ));
            ++priority;
        }
        for (FutureIterator<Entry<MemorySegment>> it : loadedIterators) {
            priorityIterators.add(new PriorityIterator<>(it, priority++));
        }

        GatheringIterator<Entry<MemorySegment>> gatheringIterator = new GatheringIterator<>(
                priorityIterators,
                Comparator.comparing(
                        it -> ((PriorityIterator<Entry<MemorySegment>>) it).showNext().key(),
                        MemorySegmentUtils::compareMemorySegments
                ).thenComparingInt(it -> ((PriorityIterator<Entry<MemorySegment>>) it).getPriority()),
                Comparator.comparing(Entry::key, MemorySegmentUtils::compareMemorySegments)
        );

        return new FutureIterator<>() {
            @Override
            public Entry<MemorySegment> showNext() {
                skipNulls();
                return gatheringIterator.showNext();
            }

            @Override
            public boolean hasNext() {
                skipNulls();
                return gatheringIterator.hasNext();
            }

            @Override
            public Entry<MemorySegment> next() {
                skipNulls();
                return gatheringIterator.next();
            }

            private void skipNulls() {
                while (gatheringIterator.hasNext() && gatheringIterator.showNext().value() == null) {
                    gatheringIterator.next();
                }
            }
        };
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memoryTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (existsPath() && !memoryTable.isEmpty()) {
            String nameSavedTable = saveMemoryTable(config.basePath());
            memoryTable.clear();
            ssTables.add(new SSTable(config.basePath(), nameSavedTable, arena));
        }
    }

    private String saveMemoryTable(Path path) throws IOException {
        FileUtils.createParentDirectories(config.basePath());

        long maxTableNumber = 0;
        for (SSTable ssTable : ssTables) {
            maxTableNumber = Math.max(maxTableNumber, Long.parseLong(ssTable.getName()));
        }
        SSTable.save(path, Long.toString(maxTableNumber + 1), memoryTable.values(), arena);

        return Long.toString(maxTableNumber + 1);
    }

    private boolean existsPath() {
        return config != null && config.basePath() != null;
    }
}
