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
import java.util.logging.Level;
import java.util.logging.Logger;

public class PersistentStorage {
    private final Arena arena;
    private final Path basePath;
    private final NavigableSet<BinarySearchSSTable> sstables = new ConcurrentSkipListSet<BinarySearchSSTable>(
            Comparator.comparingInt(ss -> -ss.id)
    );

    PersistentStorage(Path basePath) {
        this.arena = Arena.ofShared();
        this.basePath = basePath;
        try (var sstablesFiles = Files.list(basePath)) {
            sstablesFiles.filter(
                    x -> !x.getFileName().toString().contains("_index")
            ).map(
                    path -> new BinarySearchSSTable(path, arena)).forEach(this.sstables::add);
        } catch (IOException e) {
            Logger.getAnonymousLogger().log(Level.WARNING, "Failed reading SSTABLE (probably deleted)");
        }
    }

    public void close() {
        this.arena.close();
    }

    public void store(Collection<Entry<MemorySegment>> data) {
        int nextSStableID = this.sstables.isEmpty() ? 0 : this.sstables.first().id + 1;
        Path newSSTPath = BinarySearchSSTable.WriteSSTable(data, basePath, nextSStableID);
        var sstable = new BinarySearchSSTable(newSSTPath, this.arena);
        this.sstables.add(sstable);
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        for (BinarySearchSSTable sstable : this.sstables) {
            Entry<MemorySegment> ssTableResult = sstable.get(key);
            if (ssTableResult != null) {
                return ssTableResult;
            }
        }
        return null;
    }

    public List<Iterator<Entry<MemorySegment>>> get(MemorySegment from, MemorySegment to) {
        var iterators = new ArrayList<Iterator<Entry<MemorySegment>>>();
        this.sstables.forEach((var sstable) -> iterators.add(sstable.scan(from, to)));
        return iterators;
    }
}
