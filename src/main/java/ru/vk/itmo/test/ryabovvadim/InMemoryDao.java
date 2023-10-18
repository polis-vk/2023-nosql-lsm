package ru.vk.itmo.test.ryabovvadim;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

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

import static ru.vk.itmo.test.ryabovvadim.FileUtils.DATA_FILE_EXT;
import static ru.vk.itmo.test.ryabovvadim.FileUtils.ENTRY_COMPARATOR;
import static ru.vk.itmo.test.ryabovvadim.FileUtils.MEMORY_SEGMENT_COMPARATOR;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Arena arena = Arena.ofShared();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable =
        new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);
    private final Config config;
    private final List<SSTable> ssTables = new ArrayList<>();

    public InMemoryDao() {
        this(null);
    }

    public InMemoryDao(Config config) {
        this.config = config;
        if (!existsPath()) {
            return;
        }

        try {
            if (Files.notExists(config.basePath())) {
                Files.createDirectory(config.basePath());
            }
            NavigableSet<Long> dataFileNumbers = new TreeSet<>(Comparator.reverseOrder());
            Files.walkFileTree(config.basePath(), Set.of(), 1, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    String fileName = file.getFileName().toString();

                    int indexDataExtension = fileName.indexOf("." + DATA_FILE_EXT);
                    if (indexDataExtension >= 0) {
                        dataFileNumbers.add(Long.parseLong(fileName.substring(0, indexDataExtension)));
                    }

                    return FileVisitResult.CONTINUE;
                }
            });

            for (long number : dataFileNumbers) {
                ssTables.add(new SSTable(config.basePath(), Long.toString(number), arena));
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> result = memoryTable.get(key);
        
        if (result == null) {
            result = load(key);
        }
        if (result != null && result.value() == null) {
            result = null;
        }

        return result;
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
                return entry.value() == null ? null : entry;
            }
        }
        
        return null;
    }

    private FutureIterator<Entry<MemorySegment>> load(MemorySegment from, MemorySegment to) {
        List<FutureIterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        
        for (SSTable ssTable : ssTables) {
            FutureIterator<Entry<MemorySegment>> iterator = ssTable.findEntries(from, to);
            if (iterator.hasNext()) {
                iterators.add(iterator);
            }
        }
        
        return new SingleValueGatheringIterator<>(iterators, ENTRY_COMPARATOR);
    }

    private FutureIterator<Entry<MemorySegment>> makeIteratorWithSkipNulls(
            Map<MemorySegment, Entry<MemorySegment>> entries, 
            MemorySegment from, 
            MemorySegment to
    ) {
        Iterator<Entry<MemorySegment>> entriesIterator = entries.values().iterator();
        LazyIterator<Entry<MemorySegment>> futureEntriesIterator = new LazyIterator<>(
            entriesIterator::next, 
            entriesIterator::hasNext
        );
        SingleValueGatheringIterator<Entry<MemorySegment>> iterator = new SingleValueGatheringIterator<>(
            List.of(futureEntriesIterator, load(from, to)), 
            ENTRY_COMPARATOR
        );

        return new FutureIterator<Entry<MemorySegment>>() {
            @Override
            public Entry<MemorySegment> showNext() {
                skipNulls();
                return iterator.showNext();
            }

            @Override
            public boolean hasNext() {
                skipNulls();
                return iterator.hasNext();
            }

            @Override
            public Entry<MemorySegment> next() {
                skipNulls();
                return iterator.next();
            }
            
            private void skipNulls() {
                while (iterator.hasNext()) {
                    if (iterator.showNext().value() == null) {
                        iterator.next();
                    } else {
                        break;
                    }
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
        if (existsPath()) {
            save(config.basePath());
            memoryTable.clear();
        }
    }
    
    private void save(Path path) throws IOException {
        if (memoryTable.isEmpty()) {
            return;
        }
        
        FileUtils.createParentDirectories(config.basePath());
        
        long maxTableNumber = ssTables.stream()
            .map(SSTable::getName)
            .mapToLong(Long::parseLong)
            .max()
            .orElse(0);
        SSTable.save(path, Long.toString(maxTableNumber + 1), memoryTable.values(), arena);
    }
    
    private boolean existsPath() {
        return config != null && config.basePath() != null;
    }
}
