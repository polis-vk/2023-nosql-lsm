package ru.vk.itmo.mozzhevilovdanil;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

public class TablesManager {
    final long tableIndex;
    final Config config;
    private final Arena arena = Arena.ofShared();
    List<SSTable> ssTables = new ArrayList<>();

    public TablesManager(Config config) throws IOException {
        this.config = config;
        var allFiles = config.basePath().toFile().listFiles();
        if (allFiles == null) {
            tableIndex = 0;
            ssTables.add(new SSTable(arena, config, tableIndex));
            return;
        }
        tableIndex = allFiles.length / 2L;
        for (long i = tableIndex; i >= 0; i--) {
            ssTables.add(new SSTable(arena, config, i));
        }
    }

    private static FileChannel getFileChannel(Path tempIndexPath) throws IOException {
        return FileChannel.open(tempIndexPath, StandardOpenOption.WRITE, StandardOpenOption.READ,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
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

    void store(SortedMap<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        Iterator<Entry<MemorySegment>> mergeIterator = storage.values().iterator();
        arena.close();

        if (!mergeIterator.hasNext()) {
            return;
        }

        long size = 0;
        long indexSize = 0;
        List<Entry<MemorySegment>> entries = new ArrayList<>();
        while (mergeIterator.hasNext()) {
            Entry<MemorySegment> entry = mergeIterator.next();
            var entrySize = 0L;
            if (entry.value() != null) {
                entrySize = entry.value().byteSize();
            }
            size += entry.key().byteSize() + entrySize + 2L * Long.BYTES;
            indexSize += Long.BYTES;
            entries.add(entry);
        }

        try (Arena writeArena = Arena.ofConfined()) {
            MemorySegment page;
            try (FileChannel fileChannel = getFileChannel(config.basePath().resolve(tableIndex + ".db"))) {
                page = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size, writeArena);
            }
            MemorySegment index;
            try (FileChannel fileChannel = getFileChannel(config.basePath().resolve(tableIndex + ".index.db"))) {
                index = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexSize, writeArena);
            }

            long offset = 0;
            long indexOffset = 0;

            for (Entry<MemorySegment> entry : entries) {
                MemorySegment key = entry.key();

                index.set(JAVA_LONG_UNALIGNED, indexOffset, offset);
                indexOffset += Long.BYTES;

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

        }
    }

}
