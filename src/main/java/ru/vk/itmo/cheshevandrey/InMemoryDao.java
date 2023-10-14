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
import java.util.logging.Logger;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Arena offHeapArena;
    private final Path storagePath;
    private static final String STORAGE_FILE_NAME = "output.sst";
    private static final Logger logger = Logger.getLogger(InMemoryDao.class.getName());
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memTable = new ConcurrentSkipListMap<>(
            this::compare
    );

    public InMemoryDao(Config config) throws IOException {
        this.storagePath = config.basePath().resolve(STORAGE_FILE_NAME);

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

            if (ssTable.byteSize() == 0) {
                return null;
            }

            long offset = Long.BYTES;
            long mismatch;
            long operandSize;
            long tableSize = readSizeFromSsTable(ssTable, 0);
            while (offset < tableSize) {

                operandSize = readSizeFromSsTable(ssTable, offset);
                offset += Long.BYTES;

                mismatch = MemorySegment.mismatch(ssTable, offset, offset + operandSize, key, 0, key.byteSize());
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
            logger.severe("Ошибка при создании FileChannel: " + e.getMessage());
        }

        return null;
    }

    private long readSizeFromSsTable(MemorySegment ssTable, long offset) {
        return ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
    }

    @Override
    public void close() throws IOException {

        try (
                FileChannel channel = FileChannel.open(
                        storagePath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE
                );
                Arena closeArena = Arena.ofConfined()
        ) {
            long ssTableSize = 2L * Long.BYTES * memTable.size() + Long.BYTES;
            for (Entry<MemorySegment> entry : memTable.values()) {
                ssTableSize += entry.key().byteSize();
                ssTableSize += entry.value().byteSize();
            }

            MemorySegment ssTable = channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    ssTableSize,
                    closeArena
            );

            ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, ssTableSize);

            long offset = Long.BYTES;
            for (Entry<MemorySegment> entry : memTable.values()) {
                offset = storeAndGetOffset(ssTable, entry.key(), offset);
                offset = storeAndGetOffset(ssTable, entry.value(), offset);
            }
        } finally {
            offHeapArena.close();
        }
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
