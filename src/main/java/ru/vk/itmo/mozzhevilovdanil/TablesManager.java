package ru.vk.itmo.mozzhevilovdanil;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.mozzhevilovdanil.iterators.DatabaseIterator;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

public class TablesManager {
    private final Config config;
    private final Arena arena = Arena.ofShared();
    private final List<SSTable> ssTables = new ArrayList<>();
    private long tableIndex;
    private boolean isClosed;

    private static final String INDEX_PATH_SUFFIX = ".index.db";
    private static final String DB_PATH_SUFFIX = ".db";

    public TablesManager(Config config) throws IOException {
        this.config = config;
        File[] allFiles = config.basePath().toFile().listFiles();
        if (allFiles == null) {
            tableIndex = 0;
            ssTables.add(new SSTable(arena, config, tableIndex));
            return;
        }

        tableIndex = allFiles.length / 2L;
        for (long i = tableIndex; i >= 0; i--) {
            ssTables.add(new SSTable(arena, config, i));
        }

        boolean isAnyTableAlive = false;
        for (SSTable ssTable : ssTables) {
            if (ssTable.isCreated()) {
                isAnyTableAlive = true;
                break;
            }
        }

        if (!isAnyTableAlive) {
            arena.close();
        }
    }

    private static FileChannel getFileChannel(Path tempIndexPath) throws IOException {
        return FileChannel.open(tempIndexPath, StandardOpenOption.WRITE,
                StandardOpenOption.READ, StandardOpenOption.CREATE);
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry;
        for (SSTable ssTable : ssTables) {
            entry = ssTable.get(key);
            if (entry != null) {
                return entry.value() == null ? null : entry;
            }
        }
        return null;
    }

    public List<Iterator<Entry<MemorySegment>>> get(MemorySegment from, MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        for (SSTable ssTable : ssTables) {
            iterators.add(ssTable.get(from, to));
        }
        return iterators;
    }

    void compact(NavigableMap<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        tableIndex = 0;
        store(storage, true);

        File[] allFiles = config.basePath().toFile().listFiles();

        if (allFiles != null) {
            for (File file : allFiles) {
                if (file.toPath().compareTo(getIndexPath()) == 0 || file.toPath().compareTo(getDataBasePath()) == 0) {
                    continue;
                }
                Files.deleteIfExists(file.toPath());
            }
        }
        ssTables.clear();
    }

    void store(NavigableMap<MemorySegment, Entry<MemorySegment>> storage, boolean withSStables) throws IOException {
        if (isClosed) {
            return;
        }
        Iterator<Entry<MemorySegment>> iterator = storage.values().iterator();
        if (withSStables) {
            iterator = new DatabaseIterator(iterator, get(null, null));
        }

        if (arena.scope().isAlive() && !withSStables) {
            arena.close();
        }

        if (!iterator.hasNext()) {
            return;
        }

        long size = 0;
        long indexSize = 0;
        while (iterator.hasNext()) {
            Entry<MemorySegment> entry = iterator.next();
            long entrySize = 0;
            if (entry.value() != null) {
                entrySize = entry.value().byteSize();
            }
            size += entry.key().byteSize() + entrySize + 2L * Long.BYTES;
            indexSize += Long.BYTES;
        }

        Path tempDir = config.basePath().resolve("temporaryDir");
        try {
            Files.createDirectory(tempDir);
        } catch (FileAlreadyExistsException ignored) {
            // log if you need this? =)
        }

        Path tempIndexPath = tempDir.resolve(tableIndex + INDEX_PATH_SUFFIX);
        Path tempDataBasePath = tempDir.resolve(tableIndex + DB_PATH_SUFFIX);

        tryToWrite(storage, withSStables, size, indexSize, tempDataBasePath, tempIndexPath);

        Path indexPath = getDataBasePath();
        Path dataBasePath = getIndexPath();

        Files.move(tempDataBasePath, dataBasePath, StandardCopyOption.ATOMIC_MOVE);
        Files.move(tempIndexPath, indexPath, StandardCopyOption.ATOMIC_MOVE);

        Files.deleteIfExists(tempDir);

        storage.clear();
        isClosed = true;
        if (arena.scope().isAlive() && withSStables) {
            arena.close();
        }
    }

    private void tryToWrite(
            NavigableMap<MemorySegment, Entry<MemorySegment>> storage,
            boolean withSStables,
            long size,
            long indexSize,
            Path tempDataBasePath,
            Path tempIndexPath
    ) throws IOException {
        Iterator<Entry<MemorySegment>> iterator;
        try (Arena writeArena = Arena.ofConfined()) {
            MemorySegment page;

            try (FileChannel fileChannel = getFileChannel(tempIndexPath)) {
                page = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size, writeArena);
            }
            MemorySegment index;
            try (FileChannel fileChannel = getFileChannel(tempDataBasePath)) {
                index = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexSize, writeArena);
            }

            long offset = 0;
            long indexOffset = 0;

            iterator = storage.values().iterator();

            if (withSStables) {
                iterator = new DatabaseIterator(iterator, get(null, null));
            }

            while (iterator.hasNext()) {
                Entry<MemorySegment> entry = iterator.next();
                MemorySegment key = entry.key();

                page.set(JAVA_LONG_UNALIGNED, offset, key.byteSize());
                offset += Long.BYTES;

                MemorySegment value = entry.value();
                long valueSize = -1;

                if (value != null) {
                    valueSize = value.byteSize();
                }

                page.set(JAVA_LONG_UNALIGNED, offset, valueSize);
                offset += Long.BYTES;

                MemorySegment.copy(key, 0, page, offset, key.byteSize());
                offset += key.byteSize();

                if (value != null) {
                    MemorySegment.copy(value, 0, page, offset, value.byteSize());
                    offset += value.byteSize();
                }
            }

            offset = 0;

            iterator = storage.values().iterator();

            if (withSStables) {
                iterator = new DatabaseIterator(iterator, get(null, null));
            }

            while (iterator.hasNext()) {
                Entry<MemorySegment> entry = iterator.next();

                index.set(JAVA_LONG_UNALIGNED, indexOffset, offset);
                indexOffset += Long.BYTES;

                MemorySegment key = entry.key();

                offset += 2 * Long.BYTES + key.byteSize();

                MemorySegment value = entry.value();

                if (value != null) {
                    offset += value.byteSize();
                }
            }
        }
    }

    private Path getIndexPath() {
        return config.basePath().resolve(tableIndex + INDEX_PATH_SUFFIX);
    }

    private Path getDataBasePath() {
        return config.basePath().resolve(tableIndex + DB_PATH_SUFFIX);
    }
}
