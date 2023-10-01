package ru.vk.itmo.ershovvadim;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

public class PersistentDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> db =
            new ConcurrentSkipListMap<>(PersistentDaoImpl::comparator);

    private static final String TABLE_NAME = "SSTable";
    private static final String INDEX_POSTFIX = "_I";
    private final Path pathTable;
    private final Path pathIndex;

    private final MemorySegment mapTable;
    private final MemorySegment mapIndex;

    public PersistentDaoImpl(Config config) throws IOException {
        this.pathTable = config.basePath().resolve(Path.of(TABLE_NAME));
        this.pathIndex = config.basePath().resolve(Path.of(TABLE_NAME + INDEX_POSTFIX));

        if (!Files.exists(pathTable)) {
            this.mapTable = null;
            this.mapIndex = null;
            return;
        }

        try (var fcFile = FileChannel.open(pathTable, READ);
             var fcIndex = FileChannel.open(pathIndex, READ)
        ) {
            this.mapTable = fcFile.map(READ_ONLY, 0, Files.size(pathTable), Arena.global());
            this.mapIndex = fcIndex.map(READ_ONLY, 0, Files.size(pathIndex), Arena.global());
        }
    }

    private static int comparator(MemorySegment segment1, MemorySegment segment2) {
        long mismatchOffset = segment1.mismatch(segment2);
        if (mismatchOffset == -1) {
            return 0;
        } else if (mismatchOffset == segment1.byteSize()) {
            return -1;
        } else if (mismatchOffset == segment2.byteSize()) {
            return 1;
        }

        var offsetByte1 = segment1.get(JAVA_BYTE, mismatchOffset);
        var offsetByte2 = segment2.get(JAVA_BYTE, mismatchOffset);
        return Byte.compare(offsetByte1, offsetByte2);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = db.get(key);
        if (entry != null) {
            return entry;
        } else if (mapTable == null) {
            return null;
        }

        long low = 0;
        long high = mapIndex.byteSize() / Long.BYTES;

        while (low <= high) {
            long mid = low + ((high - low) / 2);

            long iterOffset = mapIndex.get(JAVA_LONG_UNALIGNED, mid * Long.BYTES);

            long msKeySize = mapTable.get(JAVA_LONG_UNALIGNED, iterOffset);
            iterOffset += Long.BYTES;
            MemorySegment findKey = mapTable.asSlice(iterOffset, msKeySize);
            iterOffset += msKeySize;

            int comparator = comparator(findKey, key);
            if (comparator < 0) {
                low = mid + 1;
            } else if (comparator > 0) {
                high = mid - 1;
            } else {
                long msValueSize = mapTable.get(JAVA_LONG_UNALIGNED, iterOffset);
                iterOffset += Long.BYTES;
                MemorySegment findValue = mapTable.asSlice(iterOffset, msValueSize);
                return new BaseEntry<>(findKey, findValue);
            }
        }
        return null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return db.values().iterator();
        } else if (to == null) {
            return db.tailMap(from).values().iterator();
        } else if (from == null) {
            return db.headMap(to).values().iterator();
        }
        return db.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        db.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (db.isEmpty()) {
            return;
        }

        long indexSize = (long) db.size() * Long.BYTES;
        long tableSize = 0;
        for (Entry<MemorySegment> entry : db.values()) {
            long valueSize = (entry.value() == null) ? 0 : entry.value().byteSize();
            tableSize += 2L * Long.BYTES + entry.key().byteSize() + valueSize;
        }

        try (var fcTable = FileChannel.open(pathTable, TRUNCATE_EXISTING, CREATE, WRITE, READ);
             var fcIndex = FileChannel.open(pathIndex, TRUNCATE_EXISTING, CREATE, WRITE, READ)
        ) {
            MemorySegment msTable = fcTable.map(READ_WRITE, 0, tableSize, Arena.global());
            MemorySegment msIndex = fcIndex.map(READ_WRITE, 0, indexSize, Arena.global());
            long indexOffset = 0;
            long tableOffset = 0;
            for (Entry<MemorySegment> entry : db.values()) {
                msIndex.set(JAVA_LONG_UNALIGNED, indexOffset, tableOffset);
                indexOffset += Long.BYTES;

                tableOffset += writeInTable(entry.key(), msTable, tableOffset);
                tableOffset += writeInTable(entry.value(), msTable, tableOffset);
            }
        }
    }

    private long writeInTable(MemorySegment key, MemorySegment file, long fileOffset) {
        long keySize = key.byteSize();
        file.set(JAVA_LONG_UNALIGNED, fileOffset, keySize);
        MemorySegment slice = file.asSlice(fileOffset + Long.BYTES, keySize);
        slice.copyFrom(key);
        return keySize + Long.BYTES;
    }
}
