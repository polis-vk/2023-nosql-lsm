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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static ru.vk.itmo.test.ryabovvadim.FileUtils.DATA_FILE_EXT;
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
        
        return new GatheringIteratorWithPriority<>(
            iterators, 
            Comparator.comparing(Entry::key, MEMORY_SEGMENT_COMPARATOR)
        );
    }

    private FutureIterator<Entry<MemorySegment>> makeIteratorWithSkipNulls(
            Map<MemorySegment, Entry<MemorySegment>> entries, MemorySegment from, MemorySegment to
    ) {
        Iterator<Entry<MemorySegment>> entriesIterator = entries.values().iterator();
        LazyIterator<Entry<MemorySegment>> futureEntriesIterator = 
            new LazyIterator<>(entriesIterator::next, entriesIterator::hasNext);
        GatheringIteratorWithPriority<Entry<MemorySegment>> iterator = new GatheringIteratorWithPriority<>(
            List.of(futureEntriesIterator, load(from, to)), 
            FileUtils.ENTRY_COMPARATOR
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
        if (config != null && config.basePath() != null) {
            save(config.basePath());
            memoryTable.clear();
        }
    }
    
    private void save(Path path) throws IOException {
        if (memoryTable.isEmpty()) {
            return;
        }
        
        FileUtils.createParentDirectories(config.basePath());
        
        int maxTableNumber = ssTables.stream()
            .map(SSTable::getName)
            .mapToInt(Integer::parseInt)
            .max()
            .orElse(0);
        int saveTableNumber = maxTableNumber + 1;

        SSTable.save(path, Integer.toString(saveTableNumber), memoryTable.values(), arena);
    }
    
    private boolean existsPath() {
        return config != null && config.basePath() != null;
    }
    
}
