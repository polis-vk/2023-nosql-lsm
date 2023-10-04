package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PersistentDao extends InMemoryDao {
    /**
     * Index file path associated with its SSTable.
     */
    private final Path indexFilePath;
    /**
     * File with SSTable path.
     */
    private final Path dataFilePath;

    private final Arena arena;
    private List<KeyPosition> indexes = null;
    private final StandardOpenOption[] openOptions = new StandardOpenOption[]{
            StandardOpenOption.WRITE,
            StandardOpenOption.READ,
            StandardOpenOption.CREATE
    };

    public PersistentDao(Path path) {
        try {
            this.indexFilePath = path.resolve("index.txt");
            this.dataFilePath = path.resolve("data.txt");
        } catch (InvalidPathException ex) {
            throw new RuntimeException("Error resolving path: " + ex.getMessage());
        }
        this.arena = Arena.ofConfined();
    }

    public record KeyPosition(long offset, long byteSize) {

        @Override
        public String toString() {
            return offset + ":" + byteSize;
        }
    }

    private long copyToFile(MemorySegment fileSegment, MemorySegment current, long offset) {
        MemorySegment.copy(current, 0, fileSegment, offset, current.byteSize());
        return current.byteSize();
    }

    @Override
    public void flush() throws IOException {
        try {
            try (FileChannel sst = FileChannel.open(dataFilePath, openOptions);
                 BufferedWriter indexFile = Files.newBufferedWriter(indexFilePath, StandardCharsets.UTF_8);
            ) {
                long offset = 0;
                for (Entry<MemorySegment> entry : entriesMap.values()) {
                    indexFile.write(String.format("%s:%s", offset, entry.key().byteSize()));
                    indexFile.newLine();
                    offset += (entry.key().byteSize() + entry.value().byteSize());
                }
                indexFile.write(String.format("%s:%s", offset, 0));
                final MemorySegment fileSegment = sst.map(FileChannel.MapMode.READ_WRITE, 0, offset, arena);
                offset = 0;
                for (Entry<MemorySegment> entry : entriesMap.values()) {
                    offset += copyToFile(fileSegment, entry.key(), offset);
                    offset += copyToFile(fileSegment, entry.value(), offset);
                }
            }
        } catch (InvalidPathException ex) {
            throw new RuntimeException("Couldn't create file by path: " + ex.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        if (arena.scope().isAlive()) {
            arena.close();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = super.get(key);
        if (entry != null) {
            return entry;
        }
        if (indexes == null) {
            indexes = new ArrayList<>();
            try (BufferedReader reader = Files.newBufferedReader(indexFilePath)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    List<Integer> pair = Arrays.stream(line.split(":")).map(Integer::parseInt).toList();
                    indexes.add(new KeyPosition(pair.get(0), pair.get(1)));
                }
            } catch (IOException e) {
                System.err.println("Couldn't open index file: " + e.getMessage());
                return null;
            }
        }
        try (FileChannel sstChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ)) {
            return entryBinarySearch(sstChannel, key);
        } catch (IOException e) {
            System.err.printf("Couldn't open file %s: %s%n", dataFilePath, e.getMessage());
        }
        return null;
    }

    private Entry<MemorySegment> entryBinarySearch(FileChannel sstFile, MemorySegment key) throws IOException {
        int l = 0;
        int r = indexes.size() - 2;
        while (l < r) {
            int index = l + (r - l) / 2;
            KeyPosition mid = indexes.get(index);
            MemorySegment middleSegment = sstFile.map(FileChannel.MapMode.READ_ONLY, mid.offset, mid.byteSize, arena);
            if (comparator.compare(key, middleSegment) <= 0) {
                r = index;
            } else {
                l = index + 1;
            }
        }
        KeyPosition sstInfo = indexes.get(l);
        MemorySegment sstKey = sstFile.map(FileChannel.MapMode.READ_ONLY, sstInfo.offset, sstInfo.byteSize, arena);
        if (comparator.compare(sstKey, key) != 0) {
            return null;
        }
        long valueOffset = sstInfo.offset + sstInfo.byteSize;
        long valueByteSize = indexes.get(l + 1).offset - valueOffset;
        return new BaseEntry<>(sstKey, sstFile.map(FileChannel.MapMode.READ_ONLY, valueOffset, valueByteSize, arena));
    }
}
