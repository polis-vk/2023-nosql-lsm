package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PersistentStorage {
    private final Path basePath;
    private final List<BinarySearchSSTable> sstables = new ArrayList<>();

    private static final class CompactionError extends RuntimeException {
        public CompactionError(Exception e) {
            super(e);
        }
    }

    PersistentStorage(Path basePath) {
        this.basePath = basePath;
        try (var sstablesFiles = Files.list(basePath)) {
            sstablesFiles.filter(
                    x -> !x.getFileName().toString().contains("_index")
            ).map(
                    path -> new BinarySearchSSTable(path, Arena.ofShared())).forEach(this.sstables::add);
        } catch (IOException e) {
            Logger.getAnonymousLogger().log(Level.WARNING, "Failed reading SSTABLE (probably deleted)");
        }
        this.sstables.sort(
                Comparator.comparingInt(o -> o.id)
        );
    }

    public void close() {
        for (var sstable : sstables) {
            sstable.close();
        }
    }

    public BinarySearchSSTable store(Collection<Entry<MemorySegment>> data) {
        int nextSStableID = this.sstables.isEmpty() ? 0 : this.sstables.getLast().id + 1;
        Path newSSTPath = BinarySearchSSTable.writeSSTable(data, basePath, nextSStableID);
        BinarySearchSSTable sstable = new BinarySearchSSTable(newSSTPath, Arena.ofShared());
        this.sstables.add(sstable);
        return sstable;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        for (BinarySearchSSTable sstable : this.sstables.reversed()) {
            if (sstable.closed) continue;
            Entry<MemorySegment> ssTableResult = sstable.get(key);
            if (ssTableResult != null) {
                return ssTableResult;
            }
        }
        return null;
    }

    public List<Iterator<Entry<MemorySegment>>> get(
            MemorySegment from,
            MemorySegment to,
            Iterator<Entry<MemorySegment>> memIterator
    ) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(sstables.size() + 1);
        iterators.add(memIterator);
        for (var sstable : sstables.reversed()) {
            if (sstable.closed) continue;
            iterators.add(sstable.scan(from, to));
        }
        return iterators;
    }

    public void compact(Iterator<Entry<MemorySegment>> data) {
        List<Entry<MemorySegment>> entries = new ArrayList<>();

        while (data.hasNext()) {
            Entry<MemorySegment> entry = data.next();
            entries.add(entry);
        }
        BinarySearchSSTable compactedTable = store(entries);

        for (var table : sstables) {
            if (table.id == compactedTable.id) {
                continue;
            }
            compactionClean(table);
        }

        sstables.clear();
        sstables.add(compactedTable);
    }

    private void compactionClean(BinarySearchSSTable sstable) {
        sstable.close();
        try {
            Files.delete(sstable.indexPath);
            Files.delete(sstable.tablePath);
        } catch (IOException e) {
            throw new CompactionError(e);
        }
    }
}
