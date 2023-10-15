package ru.vk.itmo.shemetovalexey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;


import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.logging.Logger;

class SSTable {
    private static final String INDEX_FILE_NAME = "index.db";
    private static final String DATA_FILE_NAME = "data.db";
    private final Logger logger = Logger.getLogger(SSTable.class.getName());
    private final Path dataPath;
    private final Path indexPath;
    private final Arena dataArena;
    private final Arena indexArena;
    private final MemorySegment dataFile;
    private final MemorySegment indexFile;

    public SSTable(Config config) {
        dataPath = config.basePath().resolve(DATA_FILE_NAME);
        indexPath = config.basePath().resolve(INDEX_FILE_NAME);
        indexArena = Arena.ofShared();
        dataArena = Arena.ofShared();
        long indexSize = getFileSize(indexPath);
        long dataSize = getFileSize(dataPath);
        if (indexSize == -1 || dataSize == -1) {
            dataFile = null;
            indexFile = null;
            return;
        }
        indexFile = tryOpen(indexPath, indexSize, indexArena);
        dataFile = tryOpen(dataPath, dataSize, dataArena);
        if (indexFile == null || dataFile == null) {
            logger.warning(String.format("Can't open %s", indexFile == null ? indexPath : dataPath));
            closeArena(indexArena);
            closeArena(dataArena);
        }
    }

    private static long getFileSize(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            return -1;
        }
    }

    private static MemorySegment tryOpen(Path path, long size, Arena arena) {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            return fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
        } catch (IOException e) {
            return null;
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        if (dataFile == null || indexFile == null) {
            return null;
        }
        long lIndex = -1, rIndex = indexFile.byteSize() / Long.BYTES / 3;

        while (lIndex < rIndex - 1) {
            long mid = (lIndex + rIndex) / 2 * 3 * Long.BYTES;
            long offset = getSize(indexFile, IndexType.OFFSET, mid);
            long keySize = getSize(indexFile, IndexType.KEY_SIZE, mid);
            int result = InMemoryDao.comparator(dataFile.asSlice(offset, keySize), key);
            if (result == 0) {
                long valueSize = getSize(indexFile, IndexType.VALUE_SIZE, mid);
                return new BaseEntry<>(key, dataFile.asSlice(offset + keySize, valueSize));
            } else if (result < 0) {
                lIndex = mid / 3 / Long.BYTES;
            } else {
                rIndex = mid / 3 / Long.BYTES;
            }
        }

        return null;
    }

    private static long getSize(MemorySegment memorySegment, IndexType type, long offset) {
        return memorySegment.get(ValueLayout.JAVA_LONG, offset + type.offset * Long.BYTES);
    }

    public void writeAndClose(Map<MemorySegment, Entry<MemorySegment>> memoryTable) throws IOException {
        if (dataArena == null || indexArena == null || !dataArena.scope().isAlive() || !indexArena.scope().isAlive()) {
            closeArena(dataArena);
            closeArena(indexArena);
            return;
        }
        closeArena(dataArena);
        closeArena(indexArena);

        writeIndex(memoryTable);
        writeData(memoryTable);
    }

    private void writeData(Map<MemorySegment, Entry<MemorySegment>> memoryTable) throws IOException {
        long newDataSize = 0;
        for (Entry<MemorySegment> entry : memoryTable.values()) {
            newDataSize += entry.key().byteSize() + entry.value().byteSize();
        }
        try (Arena writeArena = Arena.ofConfined()) {
            MemorySegment file = createFile(newDataSize, writeArena, dataPath);
            long offset = 0;
            for (Entry<MemorySegment> entry : memoryTable.values()) {
                MemorySegment.copy(entry.key(), 0, file, offset, entry.key().byteSize());
                offset += entry.key().byteSize();
                MemorySegment.copy(entry.value(), 0, file, offset, entry.value().byteSize());
                offset += entry.value().byteSize();
            }
        }
    }

    private MemorySegment createFile(long newSize, Arena arena, Path path) throws IOException {
        MemorySegment file;
        try (FileChannel fileChannel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            file = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, newSize, arena);
        } catch (IOException e) {
            logger.warning(String.format("Can't create file %s", path));
            throw e;
        }
        return file;
    }

    private void writeIndex(Map<MemorySegment, Entry<MemorySegment>> memoryTable) throws IOException {
        long newIndexSize = memoryTable.size() * Long.BYTES * 3L;
        try (Arena writeArena = Arena.ofConfined()) {
            MemorySegment file = createFile(newIndexSize, writeArena, indexPath);
            long offset = 0, indexOffset = 0;
            for (Entry<MemorySegment> entry : memoryTable.values()) {
                file.set(ValueLayout.JAVA_LONG, offset, indexOffset);
                offset += Long.BYTES;
                file.set(ValueLayout.JAVA_LONG, offset, entry.key().byteSize());
                offset += Long.BYTES;
                file.set(ValueLayout.JAVA_LONG, offset, entry.value().byteSize());
                offset += Long.BYTES;
            }
        }
    }

    private static void closeArena(Arena arena) {
        if (arena == null) {
            return;
        }

        if (arena.scope().isAlive()) {
            arena.close();
        }
    }

    private enum IndexType {
        OFFSET(0), KEY_SIZE(1), VALUE_SIZE(2);
        private final long offset;

        IndexType(long offset) {
            this.offset = offset;
        }
    }
}
