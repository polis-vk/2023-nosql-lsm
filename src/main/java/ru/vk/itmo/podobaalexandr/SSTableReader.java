package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.stream.Stream;

public class SSTableReader implements Closeable {

    private final List<SSTable> ssTables = new ArrayList<>();
    private Arena arena;

    public SSTableReader(Path filePath) {
        boolean created = false;

        if (Files.exists(filePath)) {
            arena = Arena.ofShared();
            try (Stream<Path> stream = Files.list(filePath)) {
                List<Path> files = stream.sorted().toList();
                for (Path file : files) {
                    SSTable ssTable = new SSTable(file, arena);
                    ssTables.addFirst(ssTable);
                    created = true;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                if (!created) {
                    arena.close();
                    arena = null;
                }
            }
        }

    }

    public int size() {
        return ssTables.size();
    }

    public Entry<MemorySegment> get(MemorySegment keySearch) {

        Entry<MemorySegment> res = null;

        for (SSTable ssTable : ssTables) {
            res = ssTable.getFromPage(keySearch);
            if (res != null) {
                if (res.value() == null) {
                    return null;
                }
                break;
            }
        }

        return res;
    }

    public Collection<Entry<MemorySegment>> allPagesFromTo(MemorySegment from, MemorySegment to,
                                ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) {
        if (size() == 0 || MemorySegmentUtils.compareSegments(from, from.byteSize(), to, 0, to.byteSize()) >= 0) {
            return map.values();
        }

        NavigableMap<MemorySegment, Entry<MemorySegment>> entries = new TreeMap<>(map);
        for (SSTable ssTable : ssTables) {
            ssTable.allPageFromTo(entries, Long.BYTES, from, to);
        }

        return entries.values();
    }

    public Collection<Entry<MemorySegment>> allPagesTo(MemorySegment to,
            ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) {
        if (size() == 0) {
            return map.values();
        }

        NavigableMap<MemorySegment, Entry<MemorySegment>> entries = new TreeMap<>(map);
        for (SSTable ssTable: ssTables) {
            ssTable.allPageTo(entries, Long.BYTES, to);
        }

        return entries.values();
    }

    public Collection<Entry<MemorySegment>> allPagesFrom(MemorySegment from,
            ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) {
        if (size() == 0) {
            return map.values();
        }

        NavigableMap<MemorySegment, Entry<MemorySegment>> entries = new TreeMap<>(map);
        for (SSTable ssTable: ssTables) {
            ssTable.allPageFrom(entries, Long.BYTES, from);
        }

        return entries.values();
    }

    public Collection<Entry<MemorySegment>> allPages(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) {
        if (size() == 0) {
            return map.values();
        }

        NavigableMap<MemorySegment, Entry<MemorySegment>> entries = new TreeMap<>(map);
        for (SSTable ssTable: ssTables) {
            ssTable.allPage(entries, Long.BYTES);
        }

        return entries.values();
    }

    @Override
    public void close() {
        arena.close();
    }

    public boolean isAlive() {
        return arena.scope().isAlive();
    }

    public boolean isArenaPresented() {
        return arena != null;
    }
}
