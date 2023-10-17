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
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static ru.vk.itmo.test.ryabovvadim.FileUtils.DATA_FILE_EXT;
import static ru.vk.itmo.test.ryabovvadim.FileUtils.MEMORY_SEGMENT_COMPARATOR;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Arena arena = Arena.ofAuto();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage =
        new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);
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
        loadIfNotExists(key);
        return storage.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        if (from == null) {
            return all();
        }
        loadIfNotExists(from, null);
        return storage.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        if (to == null) {
            return all();
        }
        loadIfNotExists(null, to);
        return storage.headMap(to).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        loadIfNotExists(null, null);
        return storage.values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            return allTo(to);
        }
        if (to == null) {
            return allFrom(from);
        }
        loadIfNotExists(from, to);
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry.value() == null) {
            storage.remove(entry.key());
        } else {
            storage.put(entry.key(), entry);
        }
        memoryTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (config != null && config.basePath() != null) {
            save(config.basePath());
        }
    }

    private void loadIfNotExists(MemorySegment key) {
        if (config == null || storage.containsKey(key) || memoryTable.containsKey(key)) {
            return;
        }

        for (SSTable ssTable : ssTables) {
            Entry<MemorySegment> entry = ssTable.findEntry(key);
            if (entry != null) {
                if (entry.value() != null) {
                    storage.put(entry.key(), entry);
                }
                return;
            }
        }
    }

    private void loadIfNotExists(MemorySegment from, MemorySegment to) {
        if (config == null) {
            return;
        }
        
        List<Entry<MemorySegment>> entriesForUpsert = new ArrayList<>();
        for (SSTable ssTable : ssTables) {
            List<Entry<MemorySegment>> entries = ssTable.findEntries(from, to);
            List<Entry<MemorySegment>> tmp = new ArrayList<>();

            int i = 0;
            int j = 0;
            while (i < entriesForUpsert.size() || j < entries.size()) {
                Entry<MemorySegment> mainEntry = i < entriesForUpsert.size() ? entriesForUpsert.get(i) : null;
                Entry<MemorySegment> newEntry = j < entries.size() ? entries.get(j) : null;
                int compareResult = MEMORY_SEGMENT_COMPARATOR.compare(
                    mainEntry == null ? null : mainEntry.key(), 
                    newEntry == null ? null : newEntry.key()
                );

                if (newEntry == null || (mainEntry != null && compareResult < 0)) {
                    tmp.add(mainEntry);
                    ++i;
                } else if (mainEntry == null || compareResult > 0) {
                    if (!storage.containsKey(newEntry.key()) && !memoryTable.containsKey(newEntry.key())) {
                        tmp.add(newEntry);
                    }
                    ++j;
                } else {
                    ++j;
                }
            }
            
            entriesForUpsert = tmp;
        }
        
        entriesForUpsert.forEach(entry -> {
            if (entry.value() != null) {
                storage.put(entry.key(), entry);
            }
        });
    }

    public void save(Path path) throws IOException {
        if (memoryTable.isEmpty()) {
            return;
        }

        synchronized (this) {
            int maxTableNumber = ssTables.stream()
                .map(SSTable::getName)
                .mapToInt(Integer::parseInt)
                .max()
                .orElse(0);

            int saveTableNumber = maxTableNumber + 1;
            SSTable.save(path, Integer.toString(saveTableNumber), memoryTable.values(), arena);
            memoryTable.clear();
        }
    }
}
