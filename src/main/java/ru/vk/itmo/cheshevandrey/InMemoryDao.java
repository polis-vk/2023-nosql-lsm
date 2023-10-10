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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Path storagePath;
    private final static String storageFileName = "output.sst";
    private final Arena offHeapArena;

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memTable = new ConcurrentSkipListMap<>(
            this::compare
    );

    public InMemoryDao(Config config) throws IOException {
        this.storagePath = config.basePath().resolve(storageFileName);

        if (!Files.exists(storagePath)) {
            Files.createDirectories(storagePath.getParent());
            Files.createFile(storagePath);
        }

        offHeapArena = Arena.ofConfined();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        if (from == null && to == null) {
            return memTable.values().iterator();
        }
        if (from == null) {
            return memTable.headMap(to).values().iterator();
        }
        if (to == null) {
            return memTable.tailMap(from).values().iterator();
        }
        return memTable.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {

        Entry<MemorySegment> entry = memTable.get(key);
        if (entry != null) {
            return entry;
        }

        try (FileChannel channel = FileChannel.open(
                storagePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ
        )) {
            MemorySegment ssTable = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    channel.size(),
                    offHeapArena
            );

            long offset = 0;
            long operandSize;
            while (offset < ssTable.byteSize()) {

                operandSize = readSizeFromSsTable(ssTable, offset);
                offset += Long.BYTES;

                long mismatch = MemorySegment.mismatch(key, 0, key.byteSize(), ssTable, offset, offset + operandSize);
                offset += operandSize;

                operandSize = readSizeFromSsTable(ssTable, offset);
                offset += Long.BYTES;

                if (mismatch == -1) {
                    return new BaseEntry<>(
                            key,
                            ssTable.asSlice(offset, operandSize)
                    );
                }
                offset += operandSize;
            }
        } catch (IOException e) {
            System.err.println("Ошибка при создании FileChannel: " + e.getMessage());
        }

        return null;
    }

    private long readSizeFromSsTable(MemorySegment ssTable, long offset) {
        return ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
    }

    @Override
    public void close() throws IOException {

        if (!offHeapArena.scope().isAlive()) {
            offHeapArena.close();
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
        }

        offHeapArena.close();
    }

    private long storeAndGetOffset(MemorySegment ssTable, MemorySegment value, long offset) {
        long newOffset = offset;
        long valueSize = value.byteSize();

        ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, newOffset, valueSize);
        newOffset += Long.BYTES;

        MemorySegment.copy(value, 0, ssTable, newOffset, valueSize);
        newOffset += valueSize;

        return newOffset;
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
