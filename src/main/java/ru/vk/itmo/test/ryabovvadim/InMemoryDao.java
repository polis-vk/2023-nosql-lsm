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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;

import static ru.vk.itmo.test.ryabovvadim.FileUtils.DATA_FILE_EXT;
import static ru.vk.itmo.test.ryabovvadim.FileUtils.MEMORY_SEGMENT_COMPARATOR;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Arena arena = Arena.ofAuto();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable =
        new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);
    private final Config config;
    private final List<SSTable> ssTables = new ArrayList<>();

    public InMemoryDao() {
        this(null);
    }

    public InMemoryDao(Config config) {
        this.config = config;
        if (config == null || config.basePath() == null) {
            return;
        }

        try {
            Set<String> dataFileNames = new HashSet<>();
            Files.walkFileTree(config.basePath(), Set.of(), 1, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    String fileName = file.getFileName().toString();

                    int indexDataExtension = fileName.indexOf("." + DATA_FILE_EXT);
                    if (indexDataExtension >= 0) {
                        dataFileNames.add(fileName.substring(0, indexDataExtension));
                    }

                    return FileVisitResult.CONTINUE;
                }
            });

            List<Integer> numbers = dataFileNames.stream()
                .map(Integer::parseInt)
                .sorted(Comparator.reverseOrder())
                .toList();
            for (var number : numbers) {
                ssTables.add(new SSTable(config.basePath(), Integer.toString(number), arena));
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
        } else if (result.value() == null) {
            result = null;
        }

        return result;
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        if (from == null) {
            return all();
        }

        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> loadEntries = load(from, null);
        loadEntries.putAll(memoryTable.tailMap(from));
        return new SkipDeletedEntriesIterator<>(loadEntries.values().iterator());
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        if (to == null) {
            return all();
        }
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> loadEntries = load(null, to); 
        loadEntries.putAll(memoryTable.headMap(to));
        return new SkipDeletedEntriesIterator<>(loadEntries.values().iterator());
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> loadEntries = load(null, null);
        loadEntries.putAll(memoryTable);
        return new SkipDeletedEntriesIterator<>(loadEntries.values().iterator());
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            return allTo(to);
        }
        if (to == null) {
            return allFrom(from);
        }

        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> loadEntries = load(from, to);
        loadEntries.putAll(memoryTable.subMap(from, to));
        return new SkipDeletedEntriesIterator<>(loadEntries.values().iterator());
    }

    private Entry<MemorySegment> load(MemorySegment key) {
        if (config == null) {
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

    private ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> load(MemorySegment from, MemorySegment to) {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> loadEntries =
            new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);

        if (config == null) {
            return loadEntries;
        }
        
        for (SSTable ssTable : ssTables) {
            for (Entry<MemorySegment> entry : ssTable.findEntries(from, to)) {
                if (!loadEntries.containsKey(entry.key())) {
                    loadEntries.put(entry.key(), entry);
                }
            }
        }
        
        return loadEntries;
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
        
        int maxTableNumber = ssTables.stream()
            .map(SSTable::getName)
            .mapToInt(Integer::parseInt)
            .max()
            .orElse(0);
        int saveTableNumber = maxTableNumber + 1;

        SSTable.save(path, Integer.toString(saveTableNumber), memoryTable.values(), arena);
    }
    
    private static class SkipDeletedEntriesIterator<T extends Entry<?>> implements Iterator<T> {
        private final Iterator<T> iterator;
        private T skippedElement = null;

        public SkipDeletedEntriesIterator(Iterator<T> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            skipNulls();
            return skippedElement != null;
        }

        @Override
        public T next() {
            skipNulls();
            T result = skippedElement;
            skippedElement = null;
            
            return result == null ? iterator.next() : result;
        }        
        
        private void skipNulls() {
            if (skippedElement != null) {
                return;
            }

            while (iterator.hasNext()) {
                T val = iterator.next();                
                
                if (val.value() != null) {
                    skippedElement = val;
                    break;
                }
            }
        }
    }
}
