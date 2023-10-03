package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private Path storagePath;

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable = new ConcurrentSkipListMap<>(
            this::compare
    );

    public InMemoryDao() {

    }

    public InMemoryDao(Config config) {
        if (config.basePath() != null) {
            this.storagePath = config.basePath().resolve("output.txt");
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        if (from == null && to == null) {
            return memTable.values().iterator();
        } else if (from == null) {
            return memTable.headMap(to).values().iterator();
        } else if (to == null) {
            return memTable.tailMap(from).values().iterator();
        } else {
            return memTable.subMap(from, to).values().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {

        if (memTable.containsKey(key)) {
            return memTable.get(key);
        } else if (storagePath != null) {

            try (FileChannel channel = FileChannel.open(
                    storagePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ
            )) {
                Arena offHeapArena = Arena.ofConfined();

                MemorySegment ssTable = channel.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        channel.size(),
                        offHeapArena
                );

                long offset = 0;
                while (ssTable.byteSize() > offset) {

                    long keySize = readSizeFromSsTable(ssTable, offset);
                    offset += Long.BYTES;
                    MemorySegment keySegment = readSegmentFromSsTable(ssTable, keySize, offset);
                    offset += keySize;

                    long valueSize = readSizeFromSsTable(ssTable, offset);
                    offset += Long.BYTES;

                    if (compare(keySegment, key) == 0) {
                        return new BaseEntry<>(
                                keySegment,
                                readSegmentFromSsTable(ssTable, valueSize, offset)
                        );
                    }

                    offset += valueSize;
                }

            } catch (IOException e) {
                return null;
            }
        }
        return null;
    }

    private long readSizeFromSsTable(MemorySegment ssTable, long offset) {
        return ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
    }

    private MemorySegment readSegmentFromSsTable(MemorySegment ssTable, long size, long offset) {
        return ssTable.asSlice(offset, size);
    }

    @Override
    public void close() throws IOException {

        if (storagePath == null) {
            return;
        }

        try (FileChannel channel = FileChannel.open(
                storagePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        )) {
            long ssTableSize = 2L * Long.BYTES * memTable.size();
            for (Entry<MemorySegment> entry : memTable.values()) {
                ssTableSize += entry.key().byteSize();
                ssTableSize += entry.value().byteSize();
            }

            Arena offHeapArena = Arena.ofConfined();

            MemorySegment ssTable = channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    ssTableSize,
                    offHeapArena
            );

            long offset = 0;
            for (Entry<MemorySegment> entry : memTable.values()) {
                offset = storeAndGetOffset(ssTable, entry.key(), offset);
                offset = storeAndGetOffset(ssTable, entry.value(), offset);
            }

            offHeapArena.close();
        }
    }

    private long storeAndGetOffset(MemorySegment ssTable,
                                  MemorySegment value,
                                  long offset) {
        long valueSize = value.byteSize();

        ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, valueSize);
        offset += Long.BYTES;

        MemorySegment.copy(value, 0, ssTable, offset, valueSize);
        offset += valueSize;

        return offset;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    private int compare(MemorySegment seg1, MemorySegment seg2) {
        long mismatch = seg1.mismatch(seg2);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == seg1.byteSize()) {
            return -1;
        }

        if (mismatch == seg2.byteSize()) {
            return 1;
        }
        byte b1 = seg1.get(ValueLayout.JAVA_BYTE, mismatch);
        byte b2 = seg2.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    }
}
