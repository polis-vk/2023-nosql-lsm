package ru.vk.itmo.pelogeikomakar;

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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final MemorySegmentComparator memorySegmentComparator = new MemorySegmentComparator();

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>>(memorySegmentComparator);

    private MemorySegment ssTable;
    private Arena ssTableArena;

    private static final String SSTABLE_NAME = "SINGLE_SS_TABLE_PELOGEIKO";
    private Config daoConfig;

    public InMemoryDao(Config config) {
        if (config == null) {
            return;
        }

        try {
            daoConfig = config;
            Path ssTablePath = config.basePath().resolve(SSTABLE_NAME);
            try (FileChannel tableFile = FileChannel.open(ssTablePath, StandardOpenOption.READ)) {
                ssTableArena = Arena.ofConfined();
                ssTable = tableFile.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(ssTablePath), ssTableArena);
            }

        } catch (IOException e) {
            ssTable = null;
            ssTableArena = null;
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (key == null) {
            return null;
        }

        Entry<MemorySegment> value = map.get(key);

        if (value == null && ssTable != null) {
            value = getFromSSTable(key);
        }

        return value;
    }

    private Entry<MemorySegment> getFromSSTable(MemorySegment key) {
        long offset = 0;
        long targetKeySize = key.byteSize();

        while (offset < ssTable.byteSize()) {
            long sizeOfKey = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            long sizeOfVal = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + Long.BYTES);
            offset += 2L * Long.BYTES;

            if (sizeOfKey == targetKeySize) {
                MemorySegment currKey = ssTable.asSlice(offset, sizeOfKey);
                if (memorySegmentComparator.compare(currKey, key) == 0) {
                    MemorySegment currValue = ssTable.asSlice(offset + sizeOfKey, sizeOfVal);
                    return new BaseEntry<MemorySegment>(currKey, currValue);
                }
            }
            offset += sizeOfKey + sizeOfVal;

        }

        return null;
    }

    @Override
    public void close() throws IOException {
        if (daoConfig == null || map.isEmpty()) {
            return;
        }

        if (ssTableArena != null) {
            ssTableArena.close();
            ssTableArena = null;
            Files.deleteIfExists(daoConfig.basePath().resolve(SSTABLE_NAME));
        }

        long ssTableSizeOut = map.size() * Long.BYTES * 2L;
        for (var item : map.values()) {
            ssTableSizeOut += item.key().byteSize() + item.value().byteSize();
        }

        FileChannel fileOut = FileChannel.open(daoConfig.basePath().resolve(SSTABLE_NAME),
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        Arena arenaWriter = Arena.ofConfined();
        MemorySegment memSegmentOut = fileOut.map(FileChannel.MapMode.READ_WRITE,
                0, ssTableSizeOut, arenaWriter);

        long offset = 0;
        for (var item : map.values()) {
            memSegmentOut.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, item.key().byteSize());
            offset += Long.BYTES;

            memSegmentOut.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, item.value().byteSize());
            offset += Long.BYTES;

            memSegmentOut.asSlice(offset, item.key().byteSize()).copyFrom(item.key());
            offset += item.key().byteSize();

            memSegmentOut.asSlice(offset, item.value().byteSize()).copyFrom(item.value());
            offset += item.value().byteSize();
        }
        arenaWriter.close();
        fileOut.close();

    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            return;
        }
        if (entry.key() == null) {
            return;
        }
        this.map.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> entryIterator;
        if (from == null && to == null) {
            entryIterator = this.map.values().iterator();
        } else if (from == null) {
            entryIterator = this.map.headMap(to).values().iterator();
        } else if (to == null) {
            entryIterator = this.map.tailMap(from).values().iterator();
        } else {
            entryIterator = this.map.subMap(from, to).values().iterator();
        }
        return entryIterator;
    }
}
