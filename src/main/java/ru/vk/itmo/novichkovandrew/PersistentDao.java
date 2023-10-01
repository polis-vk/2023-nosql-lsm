package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.*;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
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
    private final Path path;
    /**
     * Index file associated with its SSTable
     */
    private final String INDEX_FILE = "index.txt";
    /**
     * File with SSTable
     */
    private final String DATA_FILE = "sstable.txt";

    private List<KeyPosition> indexes = null;

    public PersistentDao(Path path) {
        this.path = path;
    }

    public record KeyPosition(long offset, long byteSize) {

        @Override
        public String toString() {
            return offset + ":" + byteSize;
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            Path sstPath = path.resolve(DATA_FILE);
            Path indexPath = path.resolve(INDEX_FILE);
            Files.deleteIfExists(sstPath);
            Files.deleteIfExists(indexPath);
            Files.createFile(path.resolve(DATA_FILE));
            Files.createFile(path.resolve(INDEX_FILE));
            try (BufferedOutputStream sstFile = new BufferedOutputStream(new FileOutputStream(sstPath.toFile()));
                 BufferedWriter indexFile = Files.newBufferedWriter(indexPath, StandardCharsets.UTF_8)) {
                long offset = 0;
                for (Entry<MemorySegment> entry : entriesMap.values()) {
                    MemorySegment key = entry.key();
                    MemorySegment value = entry.value();
                    indexFile.write(String.format("%s:%s", offset, key.byteSize()));
                    indexFile.newLine();
                    sstFile.write(key.toArray(ValueLayout.JAVA_BYTE));
                    sstFile.write(value.toArray(ValueLayout.JAVA_BYTE));
                    offset += (key.byteSize() + value.byteSize());
                }
                indexFile.write(String.format("%s:%s", offset, 0));
            }
        } catch (InvalidPathException ex) {
            throw new RuntimeException("Couldn't create file by path: " + ex.getMessage());
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
            try (BufferedReader reader = Files.newBufferedReader(path.resolve(INDEX_FILE))) {
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
        try (FileChannel sstChannel = FileChannel.open(path.resolve(DATA_FILE), StandardOpenOption.READ)) {
            return entryBinarySearch(sstChannel, key);
        } catch (IOException e) {
            System.err.printf("Couldn't open file %s: %s%n", DATA_FILE, e.getMessage());
        }
        return null;
    }


    private MemorySegment getNthMemorySegment(FileChannel sstChannel, long offset, long byteSize) throws IOException {
        return MemorySegment.ofBuffer(sstChannel.map(FileChannel.MapMode.READ_ONLY, offset, byteSize));
    }

    private Entry<MemorySegment> entryBinarySearch(FileChannel sstFile, MemorySegment key) throws IOException {
        int l = 0;
        int r = indexes.size() - 2;
        while (l < r) {
            int index = l + (r - l) / 2;
            KeyPosition mid = indexes.get(index);
            MemorySegment middleSegment = getNthMemorySegment(sstFile, mid.offset, mid.byteSize);
            if (comparator.compare(key, middleSegment) <= 0) {
                r = index;
            } else {
                l = index + 1;
            }
        }
        KeyPosition sstKeyPosition = indexes.get(l);
        MemorySegment sstKey = getNthMemorySegment(sstFile, sstKeyPosition.offset, sstKeyPosition.byteSize);
        if (comparator.compare(sstKey, key) != 0) {
            return null;
        }
        long valueOffset = sstKeyPosition.offset + sstKeyPosition.byteSize;
        long valueByteSize = indexes.get(l + 1).offset - valueOffset;
        return new BaseEntry<>(sstKey, getNthMemorySegment(sstFile, valueOffset, valueByteSize));
    }
}
