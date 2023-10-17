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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class PermanentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final MemorySegmentComparator memorySegmentComparator = new MemorySegmentComparator();

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> mapCurrent =
            new ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>>(memorySegmentComparator);

    private final ConcurrentMap<Integer, MemorySegment> ssTableMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Arena> arenaTableMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, MemorySegment> indexMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Arena> arenaIndexMap = new ConcurrentHashMap<>();
    private int maxSSTable = -1;
    private static final String SSTABLE_NAME = "SS_TABLE_PELOGEIKO-";
    private static final String INDEX_NAME = "INDEX_PELOGEIKO-";
    private final Config daoConfig;

    public PermanentDao(Config config) {
        if (config == null) {
            daoConfig = null;
            return;
        }

        daoConfig = config;
        Arena arenaTableCurr;
        Arena arenaIndexCurr;
        MemorySegment ssTableCurr;
        MemorySegment indexCurr;

        long filesQuantity;
        try (Stream<Path> files = Files.list(config.basePath())) {
            filesQuantity = files.count();
        } catch (IOException e) {
            filesQuantity = 0;
        }

        for (int ssTableNumber = 0; ssTableNumber < filesQuantity; ++ssTableNumber) {
            Path ssTablePath = config.basePath().resolve(SSTABLE_NAME + Integer.toString(ssTableNumber));
            Path indexPath = config.basePath().resolve(INDEX_NAME + Integer.toString(ssTableNumber));

            try {
                try (FileChannel tableFile = FileChannel.open(ssTablePath, StandardOpenOption.READ)) {
                    arenaTableCurr = Arena.ofConfined();
                    ssTableCurr = tableFile.map(FileChannel.MapMode.READ_ONLY, 0,
                            Files.size(ssTablePath), arenaTableCurr);
                }

                try (FileChannel indexFile = FileChannel.open(indexPath, StandardOpenOption.READ)) {
                    arenaIndexCurr = Arena.ofConfined();
                    indexCurr = indexFile.map(FileChannel.MapMode.READ_ONLY, 0,
                            Files.size(indexPath), arenaIndexCurr);
                }

                ssTableMap.put(ssTableNumber, ssTableCurr);
                arenaTableMap.put(ssTableNumber, arenaTableCurr);

                indexMap.put(ssTableNumber, indexCurr);
                arenaIndexMap.put(ssTableNumber, arenaIndexCurr);

            } catch (IOException e) {
                maxSSTable = ssTableNumber - 1;
                break;
            }
        }

    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (key == null) {
            return null;
        }

        Entry<MemorySegment> value = null;

        if (mapCurrent.containsKey(key)) {
            value = mapCurrent.get(key);
        } else {
            value = getFromSSTables(key);
        }

        return value;
    }

    private Entry<MemorySegment> getFromSSTables(MemorySegment key) {
        long dataOffset = -1;
        int targetTableNum = -1;
        for (int ssTableNum = maxSSTable; ssTableNum >= 0; --ssTableNum) {
            dataOffset = findKeySSTable(ssTableNum, key);
            if (dataOffset >= 0) {
                targetTableNum = ssTableNum;
                break;
            }
        }

        if (dataOffset >= 0) {
            return obtainFoundTableValue(targetTableNum, dataOffset);
        }

        return null;
    }

    private long findKeySSTable(int ssTableNum, MemorySegment key) {

        long low;
        long high;
        long result;
        MemorySegment indexCurr = indexMap.get(ssTableNum);
        MemorySegment tableCurr = ssTableMap.get(ssTableNum);

        result = -1;
        low = 0;
        high = indexCurr.byteSize() / Long.BYTES;

        while (low <= high) {
            long mid = low + ((high - low) / 2);

            long dataOffset = indexCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, mid * Long.BYTES);
            long sizeOfKey = tableCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
            MemorySegment currKey = tableCurr.asSlice(dataOffset + Long.BYTES, sizeOfKey);

            int comp = memorySegmentComparator.compare(currKey, key);

            if (comp < 0) {
                low = mid + 1;
            } else if (comp > 0) {
                high = mid - 1;
            } else {
                result = dataOffset;
                break;
            }
        }
        return result;
    }

    private Entry<MemorySegment> obtainFoundTableValue(int ssTableNum, long dataOffset) {
        MemorySegment currentTable = ssTableMap.get(ssTableNum);
        long sizeOfKey = currentTable.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
        MemorySegment currKey = currentTable.asSlice(dataOffset + Long.BYTES, sizeOfKey);
        long sizeOfVal = currentTable.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset + Long.BYTES + sizeOfKey);
        long offset = dataOffset + 2L * Long.BYTES + sizeOfKey;
        if (sizeOfVal == 0) {
            return null;
        }
        MemorySegment currValue = currentTable.asSlice(offset, sizeOfVal);
        return new BaseEntry<MemorySegment>(currKey, currValue);
    }

    @Override
    public void close() throws IOException {
        if (daoConfig == null || mapCurrent.isEmpty()) {
            return;
        }

        for (int key : arenaTableMap.keySet()) {
            arenaTableMap.get(key).close();
            arenaTableMap.remove(key);

            arenaIndexMap.get(key).close();
            arenaIndexMap.remove(key);
        }
        arenaTableMap.clear();
        arenaIndexMap.clear();
        ssTableMap.clear();
        indexMap.clear();

        long ssTableSizeOut = mapCurrent.size() * Long.BYTES * 2L;
        long indexTableSize = (long) mapCurrent.size() * Long.BYTES;
        for (var item : mapCurrent.values()) {
            ssTableSizeOut += item.key().byteSize() + (item.value() == null ? 0 : item.value().byteSize());
        }
        maxSSTable += 1;

        FileChannel fileDataOut = FileChannel.open(daoConfig.basePath()
                        .resolve(SSTABLE_NAME + Integer.toString(maxSSTable)),
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        Arena arenaDataWriter = Arena.ofConfined();
        MemorySegment memSegmentDataOut = fileDataOut.map(FileChannel.MapMode.READ_WRITE,
                0, ssTableSizeOut, arenaDataWriter);

        FileChannel fileIndexOut = FileChannel.open(daoConfig.basePath()
                        .resolve(INDEX_NAME + Integer.toString(maxSSTable)),
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        Arena arenaIndexWriter = Arena.ofConfined();
        MemorySegment memSegmentIndexOut = fileIndexOut.map(FileChannel.MapMode.READ_WRITE,
                0, indexTableSize, arenaIndexWriter);

        long offsetData = 0;
        long offsetIndex = 0;
        for (var item : mapCurrent.values()) {
            memSegmentIndexOut.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, offsetData);
            offsetIndex += Long.BYTES;

            memSegmentDataOut.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, item.key().byteSize());
            offsetData += Long.BYTES;

            MemorySegment.copy(item.key(), 0, memSegmentDataOut, offsetData, item.key().byteSize());
            offsetData += item.key().byteSize();

            if (item.value() == null) {
                memSegmentDataOut.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, 0);
                offsetData += Long.BYTES;
            } else {
                memSegmentDataOut.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, item.value().byteSize());
                offsetData += Long.BYTES;

                MemorySegment.copy(item.value(), 0, memSegmentDataOut, offsetData, item.value().byteSize());
                offsetData += item.value().byteSize();
            }

        }
        arenaDataWriter.close();
        arenaIndexWriter.close();
        fileDataOut.close();
        fileIndexOut.close();

    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            return;
        }
        if (entry.key() == null) {
            return;
        }
        this.mapCurrent.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> hashMapIter;
        if (from == null && to == null) {
            hashMapIter = this.mapCurrent.values().iterator();
        } else if (from == null) {
            hashMapIter = this.mapCurrent.headMap(to).values().iterator();
        } else if (to == null) {
            hashMapIter = this.mapCurrent.tailMap(from).values().iterator();
        } else {
            hashMapIter = this.mapCurrent.subMap(from, to).values().iterator();
        }
        List<MemorySegment> tables = new ArrayList<>();
        List<MemorySegment> indexies = new ArrayList<>();

        for (int ssTableNum = maxSSTable; ssTableNum >= 0; --ssTableNum) {
            tables.add(ssTableMap.get(ssTableNum));
            indexies.add(indexMap.get(ssTableNum));
        }
        return new DaoMergeIterator(from, to, hashMapIter, indexies, tables);
    }
}
