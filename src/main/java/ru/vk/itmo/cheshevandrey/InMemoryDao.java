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

        for (int i = ssTablesCount; i > 0; i--) {

            if (ssTables[i] == null) {
                createStorageSegment(i);
            }

            MemorySegment ssTable = ssTables[i];
            MemorySegment metaTable = metaTables[i];

            int pos = findKeyPositionOrNearest(ssTable, metaTable, key);
            int keyOffset = getKeyOffsetByIndex(metaTable, pos);
            int keySize = getKeySize(metaTable, pos, keyOffset);

            long mismatch = MemorySegment.mismatch(ssTable, keyOffset, keySize, key, 0, key.byteSize());

            if (mismatch == -1) {
                MemorySegment value = getValueSegment(ssTable, metaTable, pos);
                if (value == null) {
                    return null;
                }

                return new BaseEntry<>(
                        key,
                        value
                );
            }
        }
        return null;
    }

    static void createStorageSegment(int index) throws IOException {

        String ssTableFileName = getSsTableFileName(index);
        String ssTableMetaFileName = getSsTableMataFileName(index);
        Path ssTablePath = config.basePath().resolve(ssTableFileName);
        Path ssTableMetaPath = config.basePath().resolve(ssTableMetaFileName);

        try (
                FileChannel ssTableChannel = FileChannel.open(ssTablePath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                FileChannel metaChannel = FileChannel.open(ssTableMetaPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ) {

            MemorySegment ssTable = ssTableChannel.map(FileChannel.MapMode.READ_WRITE, 0, ssTableChannel.size(), offHeapArena);
            MemorySegment meta = metaChannel.map(FileChannel.MapMode.READ_WRITE, 0, metaChannel.size(), offHeapArena);

            ssTables[index - 1] = ssTable;
            metaTables[index - 1] = meta;

        }
    }

    static int findKeyPositionOrNearest(MemorySegment ssTable, MemorySegment meta, MemorySegment key) {
        int entryNumber = meta.get(ValueLayout.JAVA_INT_UNALIGNED, 0);

        int min = 0;
        int max = entryNumber - 1;
        int mid;
        int nearest = -1;
        while (min != max) {
            mid = (min + max) / 2;

            int keyOffset = getKeyOffsetByIndex(meta, mid);
            int keySize = getKeySize(meta, mid, keyOffset);

            /// ??? non optimise
            MemorySegment currKey = ssTable.asSlice(keyOffset, keySize);
            int compareResult = segmentComparator.compare(currKey, key);

            if (compareResult > 0) {
                max = mid - 1;
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

        createFileIfNotExists(ssTablePath);
        createFileIfNotExists(ssTableMetaPath);

        try (BufferedWriter writer = Files.newBufferedWriter(
                Paths.get(storageMetaPath.toAbsolutePath().toString()), Charset.defaultCharset()
        )) {
            writer.write(ssTablesCount + 1);
        } catch (IOException e) {
            logger.severe("Ошибка при записи количества sstable = " + ssTablesCount + ": " + e.getMessage());
            throw e;
        }

        try (
                FileChannel ssTableChannel = FileChannel.open(ssTablePath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                FileChannel metaChannel = FileChannel.open(ssTableMetaPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
        ) {
            int ssTableSize = 0;
            for (Entry<MemorySegment> entry : memTable.values()) {
                ssTableSize += (int) (entry.key().byteSize() + entry.value().byteSize());
            }

            int metaSize = Integer.BYTES + 2 * Integer.BYTES * memTable.size();

            MemorySegment ssTable = ssTableChannel.map(FileChannel.MapMode.READ_WRITE, 0, ssTableSize, offHeapArena);
            MemorySegment meta = metaChannel.map(FileChannel.MapMode.READ_WRITE, 0, metaSize, offHeapArena);

            // Сохраняем количество элементов.
            meta.set(ValueLayout.JAVA_INT_UNALIGNED, 0, memTable.size());

            metaSegmentOffset = Integer.BYTES;
            ssTableOffset = 0;
            for (Entry<MemorySegment> entry : memTable.values()) {
                saveSsTableValue(meta, ssTable, entry.key(), (int) entry.key().byteSize());
                saveSsTableValue(meta, ssTable, entry.value(), (int) entry.value().byteSize());
            }
        } finally {
            offHeapArena.close();
        }
    }

    private void saveSsTableValue(MemorySegment meta, MemorySegment ssTable, MemorySegment value, int valueSize) {
        meta.set(ValueLayout.JAVA_INT_UNALIGNED, metaSegmentOffset, ssTableOffset);
        MemorySegment.copy(value, 0, ssTable, ssTableOffset, valueSize);
        ssTableOffset += valueSize;
        metaSegmentOffset += Integer.BYTES;
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

    static int getKeyOffsetByIndex(MemorySegment meta, int index) {
        int keyPosition = Integer.BYTES + index * 2 * Integer.BYTES;
        return meta.get(ValueLayout.JAVA_INT_UNALIGNED, keyPosition);
    }

    static int getValueOffsetByIndex(MemorySegment meta, int index) {
        int valuePosition = (index + 1) * 2 * Integer.BYTES;
        return meta.get(ValueLayout.JAVA_INT_UNALIGNED, valuePosition);
    }

    static int getKeySize(MemorySegment meta, int index, int keyOffset) {
        int valueOffset = getValueOffsetByIndex(meta, index);
        return valueOffset - keyOffset;
    }

    static int getValueSize(MemorySegment meta, int index, int limitOffset) {
        int entryNumber = meta.get(ValueLayout.JAVA_INT_UNALIGNED, 0);

        int valueEndOffset;
        if (index == entryNumber - 1) {
            valueEndOffset = limitOffset;
        } else {
            valueEndOffset = getKeyOffsetByIndex(meta, index + 1);
        }
        int valueOffset = getValueOffsetByIndex(meta, index);

        return valueEndOffset - valueOffset;
    }

    static MemorySegment getKeySegment(MemorySegment ssTable, MemorySegment meta, int index) {
        int keyOffset = getKeyOffsetByIndex(meta, index);
        int keySize = getKeySize(meta, index, keyOffset);
        return ssTable.asSlice(keyOffset, keySize);
    }

    static MemorySegment getValueSegment(MemorySegment ssTable, MemorySegment meta, int index) {
        int valueOffset = getValueOffsetByIndex(meta, index);
        int valueSize = getValueSize(ssTable, index, (int) ssTable.byteSize());
        return ssTable.asSlice(valueOffset, valueSize);
    }
}
