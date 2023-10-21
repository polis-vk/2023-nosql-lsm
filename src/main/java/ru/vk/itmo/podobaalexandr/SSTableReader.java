package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.Entry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.SequencedCollection;

public class SSTableReader implements AutoCloseable {

    private final List<SSTable> ssTables = new ArrayList<>();

    private Arena arena;

    public SSTableReader(Path filePath, Path indexFile, Path indexTemp) {
        boolean created = false;

        if (Files.exists(filePath)) {

            arena = Arena.ofShared();
            try {
                if (Files.exists(indexTemp)) {
                    Files.move(indexTemp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                }

                if (!Files.exists(indexFile)) {
                    Files.createFile(indexFile);
                }

                List<String> files = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
                for (String file : files) {
                    SSTable ssTable = new SSTable(filePath.resolve(file), arena);
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

    public int ssTablesCount() {
        return ssTables.size();
    }

    public boolean isEmptySSTables() {
        return ssTablesCount() == 0;
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

    public Collection<Iterator<Entry<MemorySegment>>> iterators(MemorySegment from, MemorySegment to) {
        SequencedCollection<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        for (SSTable ssTable: ssTables) {
            Iterator<Entry<MemorySegment>> iterator = ssTable.iterator(from, to);
            if (iterator.hasNext()) {
                iterators.add(iterator);
            }
        }

        return iterators;
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
