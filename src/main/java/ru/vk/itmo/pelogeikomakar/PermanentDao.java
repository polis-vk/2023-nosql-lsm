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
    protected final MemorySegmentComparator memorySegmentComparator = new MemorySegmentComparator();

    protected final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> mapCurrent =
            new ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>>(memorySegmentComparator);

    protected final ConcurrentMap<Integer, MemorySegment> ssTableMap = new ConcurrentHashMap<>();
    protected Arena arenaTableFiles;
    protected final ConcurrentMap<Integer, MemorySegment> indexMap = new ConcurrentHashMap<>();
    protected int maxSSTable = -1;
    protected static final String SSTABLE_NAME = "SS_TABLE_PELOGEIKO-";
    protected static final String INDEX_NAME = "INDEX_PELOGEIKO-";
    protected Config daoConfig;

    public PermanentDao(Config config) {
        if (config == null) {
            daoConfig = null;
            return;
        }

        daoConfig = config;

        loadTables();
    }

    public PermanentDao() {
        daoConfig = null;
    }

    protected final void loadTables() {
        if (arenaTableFiles == null) {
            arenaTableFiles = Arena.ofShared();
        } else {
            arenaTableFiles.close();
            arenaTableFiles = Arena.ofShared();
        }
        MemorySegment ssTableCurr;
        MemorySegment indexCurr;

        long filesQuantity;
        try (Stream<Path> files = Files.list(daoConfig.basePath())) {
            filesQuantity = files.count();
        } catch (IOException e) {
            filesQuantity = 0;
        }

        for (int ssTableNumber = 0; ssTableNumber < filesQuantity; ++ssTableNumber) {
            Path ssTablePath = daoConfig.basePath().resolve(SSTABLE_NAME + Integer.toString(ssTableNumber));
            Path indexPath = daoConfig.basePath().resolve(INDEX_NAME + Integer.toString(ssTableNumber));

            try {
                try (FileChannel tableFile = FileChannel.open(ssTablePath, StandardOpenOption.READ)) {
                    ssTableCurr = tableFile.map(FileChannel.MapMode.READ_ONLY, 0,
                            Files.size(ssTablePath), arenaTableFiles);
                }

                try (FileChannel indexFile = FileChannel.open(indexPath, StandardOpenOption.READ)) {
                    indexCurr = indexFile.map(FileChannel.MapMode.READ_ONLY, 0,
                            Files.size(indexPath), arenaTableFiles);
                }

                ssTableMap.put(ssTableNumber, ssTableCurr);

                indexMap.put(ssTableNumber, indexCurr);

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
        if (value == null) {
            return null;
        }
        return value.value() == null ? null : value;
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
        if (sizeOfVal == -1) {
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

        arenaTableFiles.close();
        ssTableMap.clear();
        indexMap.clear();

        long ssTableSizeOut = mapCurrent.size() * Long.BYTES * 2L;
        long indexTableSize = (long) mapCurrent.size() * Long.BYTES;
        for (var item : mapCurrent.values()) {
            ssTableSizeOut += item.key().byteSize() + (item.value() == null ? 0 : item.value().byteSize());
        }
        maxSSTable += 1;

        Arena arenaWriter = Arena.ofShared();
        FileChannel fileDataOut = null;
        FileChannel fileIndexOut = null;

        try {
            fileDataOut = FileChannel.open(daoConfig.basePath()
                            .resolve(SSTABLE_NAME + Integer.toString(maxSSTable)),
                    StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

            MemorySegment memSegmentDataOut = fileDataOut.map(FileChannel.MapMode.READ_WRITE,
                    0, ssTableSizeOut, arenaWriter);

            fileIndexOut = FileChannel.open(daoConfig.basePath()
                            .resolve(INDEX_NAME + Integer.toString(maxSSTable)),
                    StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            MemorySegment memSegmentIndexOut = fileIndexOut.map(FileChannel.MapMode.READ_WRITE,
                    0, indexTableSize, arenaWriter);

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
                    memSegmentDataOut.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, -1);
                    offsetData += Long.BYTES;
                } else {
                    memSegmentDataOut.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, item.value().byteSize());
                    offsetData += Long.BYTES;

                    MemorySegment.copy(item.value(), 0, memSegmentDataOut, offsetData, item.value().byteSize());
                    offsetData += item.value().byteSize();
                }

            }
        } finally {
            arenaWriter.close();
            if (fileDataOut != null) {
                fileDataOut.close();
            }
            if (fileIndexOut != null) {
                fileIndexOut.close();
            }
        }
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
