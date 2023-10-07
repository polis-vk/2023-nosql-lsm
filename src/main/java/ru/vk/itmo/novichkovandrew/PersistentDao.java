package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.BufferedReader;
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
import java.util.List;

import static ru.vk.itmo.novichkovandrew.Utils.copyToSegment;
import static ru.vk.itmo.novichkovandrew.Utils.toMemorySegment;


public class PersistentDao extends InMemoryDao {
    /**
     * Path associated with SSTables.
     */
    private final Path path;

    private final Arena arena;
    private final StandardOpenOption[] openOptions = new StandardOpenOption[]{
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
    };

    public PersistentDao(Path path) {
        this.path = path;
        this.arena = Arena.ofConfined();
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
            Path sstPath = sstTablePath(Utils.filesCount(path) + 1);
            try (FileChannel sst = FileChannel.open(sstPath, openOptions)) {
                final FileChannel.MapMode mode = FileChannel.MapMode.READ_WRITE;
                long memTableSize = memTable.byteSize();
                long metaSize = memTable.getOffsetsDataSize();
                final MemorySegment indexMap = sst.map(mode, 0L, metaSize, arena);
                final MemorySegment sstMap = sst.map(mode, metaSize, memTableSize, arena);
                long indexOffset = 0L;
                long sstOffset = 0L;
                for (Entry<MemorySegment> entry : memTable) {
                    indexOffset = writePosToFile(indexMap, indexOffset, sstOffset + metaSize, entry.key().byteSize());
                    sstOffset = copyToSegment(sstMap, entry.key(), sstOffset);
                    sstOffset = copyToSegment(sstMap, entry.value(), sstOffset);
                }
                indexOffset = writePosToFile(indexMap, indexOffset, sstOffset + metaSize, 0L);
                copyToSegment(indexMap, toMemorySegment('\n'), indexOffset - 1);
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
        super.close();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = super.get(key);
        if (entry != null) {
            return entry;
        }
        long filesCount = Utils.filesCount(path);
        if (filesCount != -1) {
            for (long i = filesCount; i >= 1; i--) {
                Path sstPath = sstTablePath(i);
                if (Files.notExists(sstPath)) {
                    continue;
                }
                List<KeyPosition> indexes = getIndexList(sstPath);
                if (indexes == null) {
                    continue;
                }
                try (FileChannel sstChannel = FileChannel.open(sstPath, StandardOpenOption.READ)) {
                    entry = entryBinarySearch(sstChannel, indexes, key);
                    if (entry != null) {
                        return entry;
                    }
                } catch (IOException e) {
                    System.err.printf("Couldn't open file %s: %s%n", sstPath, e.getMessage());
                }
            }
        }
        return null;
    }


    private List<KeyPosition> getIndexList(Path file) {
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String[] chunks = reader.readLine().split(" ");
            List<KeyPosition> list = new ArrayList<>(chunks.length);
            for (String chunk : chunks) {
                String[] keyData = chunk.split(":");
                list.add(new KeyPosition(Long.parseLong(keyData[0]), Long.parseLong(keyData[1])));
            }
            return list;
        } catch (IOException e) {
            System.err.printf("Couldn't read meta-data of sst table by path %s: %s%n", file, e.getMessage());
        }
        return null;
    }

    private Entry<MemorySegment> entryBinarySearch(FileChannel sst, List<KeyPosition> indexes,
                                                   MemorySegment key) throws IOException {
        int l = 0;
        int r = indexes.size() - 2;
        while (l < r) {
            int index = l + (r - l) / 2;
            KeyPosition mid = indexes.get(index);
            MemorySegment middleSegment = sst.map(FileChannel.MapMode.READ_ONLY, mid.offset, mid.byteSize, arena);
            if (comparator.compare(key, middleSegment) <= 0) {
                r = index;
            } else {
                l = index + 1;
            }
        }
        KeyPosition sstInfo = indexes.get(l);
        MemorySegment sstKey = sst.map(FileChannel.MapMode.READ_ONLY, sstInfo.offset, sstInfo.byteSize, arena);
        if (comparator.compare(sstKey, key) != 0) {
            return null;
        }
        long valueOffset = sstInfo.offset + sstInfo.byteSize;
        long valueByteSize = indexes.get(l + 1).offset - valueOffset;
        return new BaseEntry<>(sstKey, sst.map(FileChannel.MapMode.READ_ONLY, valueOffset, valueByteSize, arena));
    }

    private long writePosToFile(MemorySegment fileSegment, long indexOffset, long offset, long byteSize) {
        indexOffset = copyToSegment(fileSegment, toMemorySegment(offset), indexOffset);
        indexOffset = copyToSegment(fileSegment, toMemorySegment(':'), indexOffset);
        indexOffset = copyToSegment(fileSegment, toMemorySegment(byteSize), indexOffset);
        return copyToSegment(fileSegment, toMemorySegment(' '), indexOffset);
    }

    private Path sstTablePath(long suffix) {
        String fileName = String.format("data-%s.txt", suffix);
        return path.resolve(Path.of(fileName));
    }
}
