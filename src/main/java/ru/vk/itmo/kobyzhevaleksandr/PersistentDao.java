package ru.vk.itmo.kobyzhevaleksandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> map =
        new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    private static final String TABLE_FILENAME = "ssTable.dat";

    private final Arena arena = Arena.ofShared();
    private final Config config;
    private final MemorySegment mappedSsTable;

    public PersistentDao() {
        this(new Config(Path.of("")));
    }

    /*
    Filling ssTable with bytes from the memory segment with a structure:
    [key_size][key][value_size][value]...

    If value is null then value_size = -1
     */
    public PersistentDao(Config config) {
        this.config = config;
        Path tablePath = getTablePath();
        MemorySegment mappedTableSegment;
        try {
            long size = Files.size(tablePath);
            mappedTableSegment = mapFile(tablePath, size, FileChannel.MapMode.READ_ONLY,
                arena, StandardOpenOption.READ);
        } catch (NoSuchFileException e) {
            mappedTableSegment = null;
        } catch (IOException e) {
            throw new ApplicationException("Can't access the file", e);
        }
        mappedSsTable = mappedTableSegment;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return map.values().iterator();
        } else if (from == null) {
            return map.headMap(to).values().iterator();
        } else if (to == null) {
            return map.tailMap(from).values().iterator();
        }
        return map.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = map.get(key);
        if (entry != null) {
            return entry;
        }
        if (mappedSsTable == null) {
            return null;
        }

        long offset = 0;
        while (offset < mappedSsTable.byteSize()) {
            long keySize = mappedSsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            long valueSize = mappedSsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + keySize);

            if (keySize != key.byteSize()) {
                offset += keySize + Long.BYTES + valueSize;
                continue;
            }

            if (MemorySegment.mismatch(key, 0, keySize, mappedSsTable, offset, offset + keySize) == -1) {
                MemorySegment value;
                if (valueSize == -1) {
                    value = null;
                } else {
                    value = mappedSsTable.asSlice(offset + keySize + Long.BYTES, valueSize);
                }
                return new BaseEntry<>(key, value);
            }
            offset += keySize + Long.BYTES + valueSize;
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Entry cannot be null.");
        }
        map.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();

        try (Arena writeArena = Arena.ofConfined()) {
            long ssTableSize = 0;
            for (Entry<MemorySegment> entry : map.values()) {
                long valueSize = entry.value() == null ? 0 : entry.value().byteSize();
                ssTableSize += Long.BYTES + entry.key().byteSize() + Long.BYTES + valueSize;
            }

            Path tablePath = getTablePath();
            MemorySegment mappedSsTableFile = mapFile(tablePath, ssTableSize,
                FileChannel.MapMode.READ_WRITE, writeArena,
                StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            long offset = 0;
            for (Entry<MemorySegment> entry : map.values()) {
                offset += writeSegmentToMappedTableFile(mappedSsTableFile, entry.key(), offset);
                offset += writeSegmentToMappedTableFile(mappedSsTableFile, entry.value(), offset);
            }
        }
    }

    private Path getTablePath() {
        return config.basePath().resolve(TABLE_FILENAME);
    }

    private static MemorySegment mapFile(Path filePath, long bytesSize, FileChannel.MapMode mapMode, Arena arena,
                                  OpenOption... options) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(filePath, options)) {
            return fileChannel.map(mapMode, 0, bytesSize, arena);
        }
    }

    private static long writeSegmentToMappedTableFile(MemorySegment mappedTableFile,
                                                      MemorySegment segment, long offset) {
        if (segment == null) {
            mappedTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, -1);
            return Long.BYTES;
        }
        mappedTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, segment.byteSize());
        MemorySegment.copy(segment, 0, mappedTableFile, offset + Long.BYTES, segment.byteSize());
        return Long.BYTES + segment.byteSize();
    }
}
