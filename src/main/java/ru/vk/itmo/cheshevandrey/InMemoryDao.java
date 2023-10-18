package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Logger;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final String META_FILE_NAME = "storage-meta.sst";
    private static final String SSTABLE_FILE_NAME_PREFIX = "ssTable_";
    private static final String SSTABLE_META_FILE_NAME_PREFIX = "ssTable_meta_";
    private static final String SSTABLE_FILE_EXTENSION = ".sst";

    private static final Logger logger = Logger.getLogger(InMemoryDao.class.getName());

    static Config config;

    private final Path ssTablePath;
    private final Path ssTableMetaPath;
    private final Path storageMetaPath;

    static final SegmentComparator segmentComparator = new SegmentComparator();
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memTable = new ConcurrentSkipListMap<>(segmentComparator);

    private final int ssTablesCount;

    static MemorySegment[] ssTables;
    static MemorySegment[] metaTables;

    private static Arena offHeapArena;

    private int metaSegmentOffset;
    private int ssTableOffset;

    @SuppressWarnings("StaticAssignmentInConstructor")
    public InMemoryDao(Config config) throws IOException {

        InMemoryDao.config = config;

        this.storageMetaPath = config.basePath().resolve(META_FILE_NAME);
        ssTablesCount = getSsTablesCount();

        String ssTableFileName = getSsTableFileName(ssTablesCount + 1);
        String ssTableMetaFileName = getSsTableMataFileName(ssTablesCount + 1);

        this.ssTablePath = config.basePath().resolve(ssTableFileName);
        this.ssTableMetaPath = config.basePath().resolve(ssTableMetaFileName);

        ssTables = new MemorySegment[ssTablesCount];
        metaTables = new MemorySegment[ssTablesCount];

        offHeapArena = Arena.ofConfined();
    }

    static String getSsTableFileName(int ssTableNumber) {
        return SSTABLE_FILE_NAME_PREFIX + ssTableNumber + SSTABLE_FILE_EXTENSION;
    }

    static String getSsTableMataFileName(int ssTableNumber) {
        return SSTABLE_META_FILE_NAME_PREFIX + ssTableNumber + SSTABLE_FILE_EXTENSION;
    }

    private int getSsTablesCount() throws IOException {

        int result = 0;
        if (Files.exists(storageMetaPath)) {

            try (BufferedReader reader = Files.newBufferedReader(
                    Paths.get(storageMetaPath.toAbsolutePath().toString()), Charset.defaultCharset()
            )) {
                result = reader.read();
            }
        }

        return result;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        Iterator<Entry<MemorySegment>> memTableIterator = getMemTableIterator(from, to);

        try {
            return new InMemoryIterator(memTableIterator, ssTablesCount, from, to);
        } catch (IOException e) {
            logger.severe("Ошибка при создании итератора" + e.getMessage());
        }
        return null;
    }

    private Iterator<Entry<MemorySegment>> getMemTableIterator(MemorySegment from, MemorySegment to) {
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
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        try {
            return findKeyInStorage(key);
        } catch (IOException e) {
            logger.severe("Ошибка при поиске ключа: " + e.getMessage());
        }

        return null;
    }

    private Entry<MemorySegment> findKeyInStorage(MemorySegment key) throws IOException {

        for (int i = ssTablesCount - 1; i >= 0; i--) {

            if (ssTables[i] == null) {
                createStorageSegment(i);
            }

            MemorySegment ssTable = ssTables[i];
            MemorySegment metaTable = metaTables[i];

            int pos = findKeyPositionOrNearest(ssTable, metaTable, key);
            int keyOffset = getKeyOffsetByIndex(metaTable, pos);
            int keySize = getKeySize(ssTable, metaTable, pos, keyOffset);

            long mismatch = MemorySegment.mismatch(ssTable, keyOffset, keyOffset + keySize, key, 0, key.byteSize());

            if (mismatch == -1) {

                MemorySegment value = getValueSegment(ssTable, metaTable, pos);
                return isNullValue(metaTable, pos) ? null : new BaseEntry<>(key, value);
            }
        }
        return null;
    }

    static void createStorageSegment(int index) throws IOException {

        String ssTableFileName = getSsTableFileName(index + 1);
        String ssTableMetaFileName = getSsTableMataFileName(index + 1);
        Path ssTablePath = config.basePath().resolve(ssTableFileName);
        Path ssTableMetaPath = config.basePath().resolve(ssTableMetaFileName);

        try (
                FileChannel ssTableChannel = FileChannel.open(ssTablePath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                FileChannel metaChannel = FileChannel.open(ssTableMetaPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ) {

            MemorySegment ssTable = ssTableChannel.map(FileChannel.MapMode.READ_WRITE, 0, ssTableChannel.size(), offHeapArena);
            MemorySegment meta = metaChannel.map(FileChannel.MapMode.READ_WRITE, 0, metaChannel.size(), offHeapArena);

            ssTables[index] = ssTable;
            metaTables[index] = meta;

        }
    }

    static int findKeyPositionOrNearest(MemorySegment ssTable, MemorySegment meta, MemorySegment key) {
        int entryNumber = meta.get(ValueLayout.JAVA_INT_UNALIGNED, 0);

        int min = 0;
        int max = entryNumber;
        int nearest = -1;
        while (min != max) {
            int mid = (min + max) / 2;

            int keyOffset = getKeyOffsetByIndex(meta, mid);
            int keySize = getKeySize(ssTable, meta, mid, keyOffset);

            /// ??? non optimise
            MemorySegment currKey = ssTable.asSlice(keyOffset, keySize);
            int compareResult = segmentComparator.compare(currKey, key);

            if (compareResult > 0) {
                max = mid;
            } else if (compareResult < 0) {
                nearest = mid;
                min = mid + 1;
            } else {
                return mid;
            }
        }

        return nearest;
    }

    @Override
    public void flush() throws IOException {

        if (memTable.isEmpty()) {
            return;
        }

        createFileIfNotExists(ssTablePath);
        createFileIfNotExists(ssTableMetaPath);

        try (
                FileChannel storageMetaChannel = FileChannel.open(storageMetaPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                FileChannel ssTableChannel = FileChannel.open(ssTablePath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                FileChannel metaChannel = FileChannel.open(ssTableMetaPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
        ) {
            MemorySegment storageMeta = storageMetaChannel.map(FileChannel.MapMode.READ_WRITE, 0, Integer.BYTES, offHeapArena);
            // Сохраняем количество ssTable.
            storageMeta.set(ValueLayout.JAVA_INT_UNALIGNED, 0, ssTablesCount + 1);

            int ssTableSize = 0;
            for (Entry<MemorySegment> entry : memTable.values()) {
                ssTableSize += (int) entry.key().byteSize();
                if (entry.value() != null) {
                    ssTableSize += (int) entry.value().byteSize();
                }
            }

            // Вычисляем длину текущей мета-таблицы.
            int metaSize = Integer.BYTES * (1 + 2 * memTable.size());

            MemorySegment ssTable = ssTableChannel.map(FileChannel.MapMode.READ_WRITE, 0, ssTableSize, offHeapArena);
            MemorySegment meta = metaChannel.map(FileChannel.MapMode.READ_WRITE, 0, metaSize, offHeapArena);

            // Сохраняем количество элементов ssTable в мета-таблицу.
            meta.set(ValueLayout.JAVA_INT_UNALIGNED, 0, memTable.size());

            // Записываем entry в ssTable.
            metaSegmentOffset = Integer.BYTES;
            ssTableOffset = 0;
            for (Entry<MemorySegment> entry : memTable.values()) {

                int keySize = (int) entry.key().byteSize();
                meta.set(ValueLayout.JAVA_INT_UNALIGNED, metaSegmentOffset, ssTableOffset);
                MemorySegment.copy(entry.key(), 0, ssTable, ssTableOffset, keySize);
                metaSegmentOffset += Integer.BYTES;

                if (entry.value() != null) {
                    ssTableOffset += keySize;
                    int valueSize = (int) entry.value().byteSize();
                    meta.set(ValueLayout.JAVA_INT_UNALIGNED, metaSegmentOffset, ssTableOffset);
                    MemorySegment.copy(entry.value(), 0, ssTable, ssTableOffset, valueSize);
                    ssTableOffset += valueSize;
                } else {
                    meta.set(ValueLayout.JAVA_INT_UNALIGNED, metaSegmentOffset, ssTableOffset);
                    ssTableOffset += keySize;
                }
                metaSegmentOffset += Integer.BYTES;
            }
        } finally {
            if (offHeapArena.scope().isAlive()) {
                offHeapArena.close();
            }
        }
    }

    private void createFileIfNotExists(Path path) throws IOException {
        if (!Files.exists(path)) {
            Files.createDirectories(path.getParent());
            Files.createFile(path);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }


    static boolean isNullValue(MemorySegment meta, int index) {
        int keyOffset = getKeyOffsetByIndex(meta, index);
        int valueOffset = getValueOffsetByIndex(meta, index);
        return keyOffset == valueOffset;
    }

    static int getKeyOffsetByIndex(MemorySegment meta, int index) {
        int keyPosition = (1 + index * 2) * Integer.BYTES;
        return meta.get(ValueLayout.JAVA_INT_UNALIGNED, keyPosition);
    }

    static int getValueOffsetByIndex(MemorySegment meta, int index) {
        int valuePosition = (index + 1) * 2 * Integer.BYTES;
        return meta.get(ValueLayout.JAVA_INT_UNALIGNED, valuePosition);
    }

    static int getKeySize(MemorySegment ssTable, MemorySegment meta, int index, int keyOffset) {
        int endOffset;
        if (isNullValue(meta, index)) {
            endOffset = getLimitValueOffset(meta, index, (int) ssTable.byteSize());
        } else {
            endOffset = getValueOffsetByIndex(meta, index);
        }
        return endOffset - keyOffset;
    }

    static int getLimitValueOffset(MemorySegment meta, int index, int limitOffset) {
        int entryNumber = meta.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        int valueEndOffset;
        if (index == entryNumber - 1) {
            valueEndOffset = limitOffset;
        } else {
            valueEndOffset = getKeyOffsetByIndex(meta, index + 1);
        }
        return valueEndOffset;
    }

    static int getValueSize(MemorySegment meta, int index, int limitOffset) {

        int valueEndOffset = getLimitValueOffset(meta, index, limitOffset);
        int valueOffset = getValueOffsetByIndex(meta, index);

        return valueEndOffset - valueOffset;
    }

    static MemorySegment getKeySegment(MemorySegment ssTable, MemorySegment meta, int index) {
        int keyOffset = getKeyOffsetByIndex(meta, index);
        int keySize = getKeySize(ssTable, meta, index, keyOffset);
        return ssTable.asSlice(keyOffset, keySize);
    }

    static MemorySegment getValueSegment(MemorySegment ssTable, MemorySegment meta, int index) {
        int valueOffset = getValueOffsetByIndex(meta, index);
        int valueSize = getValueSize(meta, index, (int) ssTable.byteSize());
        return ssTable.asSlice(valueOffset, valueSize);
    }
}
