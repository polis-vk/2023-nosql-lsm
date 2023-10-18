package ru.vk.itmo.ershovvadim;

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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

public class PersistentDaoImpl extends AbstractMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

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
            this.mapTable = fcFile.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(pathTable), Arena.ofConfined());
            this.mapIndex = fcIndex.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(pathIndex), Arena.ofConfined());
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = db.get(key);
        if (entry != null) {
            return entry;
        }
        if (mapTable == null) {
            return null;
        }

        long low = 0;
        long high = mapIndex.byteSize() / Long.BYTES - 1;

        while (low <= high) {
            long mid = low + ((high - low) / 2);

            long iterOffset = mapIndex.get(ValueLayout.JAVA_LONG_UNALIGNED, mid * Long.BYTES);

            long msKeySize = mapTable.get(ValueLayout.JAVA_LONG_UNALIGNED, iterOffset);
            iterOffset += Long.BYTES;
            MemorySegment findKey = mapTable.asSlice(iterOffset, msKeySize);
            iterOffset += msKeySize;

            int comparator = super.compare(findKey, key);
            if (comparator < 0) {
                low = mid + 1;
            } else if (comparator > 0) {
                high = mid - 1;
            } else {
                long msValueSize = mapTable.get(ValueLayout.JAVA_LONG_UNALIGNED, iterOffset);
                iterOffset += Long.BYTES;
                MemorySegment findValue = mapTable.asSlice(iterOffset, msValueSize);
                return new BaseEntry<>(findKey, findValue);
            }
        }
        return null;
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
            MemorySegment msTable = fcTable.map(FileChannel.MapMode.READ_WRITE, 0, tableSize, Arena.ofConfined());
            MemorySegment msIndex = fcIndex.map(FileChannel.MapMode.READ_WRITE, 0, indexSize, Arena.ofConfined());
            long indexOffset = 0;
            long tableOffset = 0;
            for (Entry<MemorySegment> entry : db.values()) {
                msIndex.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tableOffset);
                indexOffset += Long.BYTES;

                tableOffset += writeInTable(entry.key(), msTable, tableOffset);
                tableOffset += writeInTable(entry.value(), msTable, tableOffset);
            }
        }
    }

    private long writeInTable(MemorySegment key, MemorySegment file, long fileOffset) {
        long keySize = key.byteSize();
        file.set(ValueLayout.JAVA_LONG_UNALIGNED, fileOffset, keySize);
        MemorySegment slice = file.asSlice(fileOffset + Long.BYTES, keySize);
        slice.copyFrom(key);
        return keySize + Long.BYTES;
    }
}
