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
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PersistentStorage {
    private final Path basePath;
    private final NavigableSet<BinarySearchSSTable> sstables = new ConcurrentSkipListSet<>(
            Comparator.comparingInt(o -> o.id)
    );
    private final AtomicInteger lastSSTableId;

    private final Arena arena;

    private static final class CompactionError extends RuntimeException {
        public CompactionError(Exception e) {
            super(e);
        }
    }

    PersistentStorage(Path basePath) {
        arena = Arena.ofShared();
        this.basePath = basePath;
        try (var sstablesFiles = Files.list(basePath)) {
            sstablesFiles.filter(
                    x -> !x.getFileName().toString().contains("_index") && !x.getFileName().toString().contains("tmp")
            ).map(
                    path -> new BinarySearchSSTable(path, arena)).forEach(this.sstables::add);
        } catch (IOException e) {
            Logger.getAnonymousLogger().log(Level.WARNING, "Failed reading SSTABLE (probably deleted)");
        }

        lastSSTableId = new AtomicInteger(this.sstables.isEmpty() ? 0 : this.sstables.getLast().id);
    }

    public void close() {
        arena.close();
    }

    /**
     * Гарантирует что при успешном завершении записи на диск, SSTable с переданными в метод данными
     * сразу будет доступен для чтения в PersistentStorage.
     **/
    public void store(Collection<Entry<MemorySegment>> data) {
        int nextSStableID = this.lastSSTableId.incrementAndGet();
        BinarySearchSSTable newSSTable = BinarySearchSSTable.writeSSTable(data, basePath, nextSStableID, arena);
        this.sstables.add(newSSTable);
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        for (BinarySearchSSTable sstable : this.sstables.reversed()) {
            if (sstable.closed.get()) continue;
            Entry<MemorySegment> ssTableResult = sstable.get(key);
            if (ssTableResult != null) {
                return ssTableResult;
            }
        }
        return null;
    }

    public void enrichWithPersistentIterators(
            MemorySegment from,
            MemorySegment to,
            List<Iterator<Entry<MemorySegment>>> iteratorsToEnrich
    ) {
        iteratorsToEnrich.addAll(getPersistentIterators(from, to));
    }

    private List<Iterator<Entry<MemorySegment>>> getPersistentIterators(
            MemorySegment from,
            MemorySegment to
    ) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(sstables.size() + 1);
        for (var sstable : sstables.reversed()) {
            if (sstable.closed.get()) continue;
            iterators.add(sstable.scan(from, to));
        }
        return iterators;
    }

    private List<BinarySearchSSTable> getCompactableTables() {
        List<BinarySearchSSTable> res = new ArrayList<>(sstables.size());
        for (var sstable : sstables.reversed()) {
            if (sstable.closed.get()) continue;
            if (sstable.inCompaction.compareAndSet(false, true)) continue;
            res.add(sstable);
        }
        return res;
    }

    public void compact() {
        List<BinarySearchSSTable> tablesToCompact = getCompactableTables();

        List<Entry<MemorySegment>> entries = new ArrayList<>();

        for (var sstable : tablesToCompact) {
            Iterator<Entry<MemorySegment>> tableEntries = sstable.scan(null, null);
            while (tableEntries.hasNext()) {
                entries.add(tableEntries.next());
            }
        }
        store(entries);
        for (var sstable : tablesToCompact) {
            compactionClean(sstable);
        }
    }

    private void compactionClean(BinarySearchSSTable sstable) {
        try {
            Files.delete(sstable.indexPath);
            Files.delete(sstable.tablePath);
        } catch (IOException e) {
            throw new CompactionError(e);
        }
    }
}
