package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.Entry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.SequencedCollection;

public class SSTableReader implements AutoCloseable {

    private static final StandardCopyOption[] options =
            {StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING};

    private final Path filePath;
    private final Path indexFile;
    private final Path indexTemp;

    private final Arena arena;

    public SSTableReader(Path filePath, Path indexFile, Path indexTemp) {
        this.indexFile = indexFile;
        this.indexTemp = indexTemp;
        this.filePath = filePath;
        arena = Arena.ofShared();
    }

    private List<SSTable> readIndexFile() throws IOException {
        if (Files.exists(indexTemp)) {
            Files.move(indexTemp, indexFile, options);
        }

        if (!Files.exists(indexFile)) {
            return new ArrayList<>();
        }

        List<String> files = Files.readAllLines(indexFile);

        List<SSTable> ssTables = new ArrayList<>(files.size());
        for (String file : files) {
            SSTable ssTable = new SSTable(filePath.resolve(file), arena);
            ssTables.addFirst(ssTable);
        }

        return ssTables;
    }

    public Entry<MemorySegment> get(MemorySegment keySearch) {

        Entry<MemorySegment> res = null;

        try {
            List<SSTable> ssTables = readIndexFile();

            for (SSTable ssTable : ssTables) {
                res = ssTable.getEntryFromPage(keySearch);
                if (res != null) {
                    if (res.value() == null) {
                        return null;
                    }
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return res;
    }

    public Collection<Iterator<Entry<MemorySegment>>> iterators(MemorySegment from, MemorySegment to) {
        SequencedCollection<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();

        try {
            List<SSTable> ssTables = readIndexFile();

            for (SSTable ssTable : ssTables) {
                Iterator<Entry<MemorySegment>> iterator = ssTable.iterator(from, to);
                if (iterator.hasNext()) {
                    iterators.add(iterator);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
