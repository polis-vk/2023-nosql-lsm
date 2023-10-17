package ru.vk.itmo.pelogeikomakar;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

public class DaoMergeIterator implements Iterator<Entry<MemorySegment>> {

    private final MemorySegmentComparator comparator = new MemorySegmentComparator();
    private final MemorySegment from;
    private final MemorySegment to;
    private final Iterator<Entry<MemorySegment>> hashMapIter;
    private Entry<MemorySegment> currentHashElem;
    private final List<MemorySegment> indexList;
    private final Map<Integer, Long> indexOffsetMap = new HashMap<>();
    private final List<MemorySegment> tableList;

    public DaoMergeIterator(MemorySegment from, MemorySegment to,
                            Iterator<Entry<MemorySegment>> hashMapIter,
                            List<MemorySegment> indexList,
                            List<MemorySegment> tableList) {
        this.to = to;
        this.from = from;
        this.hashMapIter = hashMapIter;
        this.indexList = indexList;
        this.tableList = tableList;
    }

    @Override
    public boolean hasNext() {
        if (hashMapIter.hasNext()) {
            return true;
        }

        if (currentHashElem != null) {
            return true;
        }

        return getNextTable() != -1;
    }

    private int getNextTable() {

        int minTableIndex = -1;
        MemorySegment minKey = null;

        for (int i = 0; i < indexList.size(); ++i) {
            if (!indexOffsetMap.containsKey(i)) {
                indexOffsetMap.put(i, getStartOffset(i, from));
            }
            if (indexOffsetMap.get(i) < 0) {
                continue;
            }
            long dataOffset = indexList.get(i).get(ValueLayout.JAVA_LONG_UNALIGNED, indexOffsetMap.get(i));
            long sizeOfKey = tableList.get(i).get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
            MemorySegment currKey = tableList.get(i).asSlice(dataOffset + Long.BYTES, sizeOfKey);
            if (to != null && comparator.compare(currKey, to) > 0) {
                continue;
            }
            if (minKey == null) {
                minTableIndex = i;
                minKey = currKey;
            } else if (comparator.compare(minKey, currKey) > 0) {
                minTableIndex = i;
                minKey = currKey;
            } else if (comparator.compare(minKey, currKey) == 0) {
                stepToNextInTable(i);
            }
        }
        return minTableIndex;
    }

    private long getStartOffset(int table, MemorySegment key) {
        if (from == null) {
            return 0L;
        }

        long result;
        long low;
        long high;
        result = -1;
        low = 0;
        var indexCurr = indexList.get(table);
        var tableCurr = tableList.get(table);
        high = indexCurr.byteSize() / Long.BYTES;

        while (low <= high) {
            long mid = low + ((high - low) / 2);

            long dataOffset = indexCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, mid * Long.BYTES);
            long sizeOfKey = tableCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
            MemorySegment currKey = tableCurr.asSlice(dataOffset + Long.BYTES, sizeOfKey);

            int comp = comparator.compare(currKey, key);

            if (low == high) {
                if (comp == 0) {
                    return high * Long.BYTES;
                }
                mid -= 1;
                dataOffset = indexCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, mid * Long.BYTES);
                sizeOfKey = tableCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
                currKey = tableCurr.asSlice(dataOffset + Long.BYTES, sizeOfKey);
                comp = comparator.compare(currKey, key);
                if (comp < 0) {
                    return mid * Long.BYTES;
                }
                return -1L;
            }

            if (comp < 0) {
                low = mid + 1;
            } else if (comp > 0) {
                high = mid - 1;
            } else {
                result = mid * Long.BYTES;
                break;
            }
        }
        return result;
    }

    private List<MemorySegment> getNextFromTable(int tableIdx) {
        long dataOffset = indexList.get(tableIdx).get(ValueLayout.JAVA_LONG_UNALIGNED, indexOffsetMap.get(tableIdx));
        var table = tableList.get(tableIdx);
        long sizeOfKey = table.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
        long sizeOfVal = table.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset + Long.BYTES + sizeOfKey);
        long valOffset = dataOffset + 2L * Long.BYTES + sizeOfKey;
        MemorySegment key = table.asSlice(dataOffset + Long.BYTES, sizeOfKey);
        MemorySegment value = table.asSlice(valOffset, sizeOfVal);
        List<MemorySegment> result = new ArrayList<>();
        result.add(key);
        result.add(value);

        return result;
    }

    private void stepToNextInTable(int tableIdx) {
        indexOffsetMap.put(tableIdx, indexOffsetMap.get(tableIdx) + Long.BYTES);

        if (indexOffsetMap.get(tableIdx) == indexList.get(tableIdx).byteSize()) {
            indexOffsetMap.put(tableIdx, -1L);
        }
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            return null;
        }
        int table = getNextTable();

        if (currentHashElem == null) {
            if (hashMapIter.hasNext()) {
                currentHashElem = hashMapIter.next();
            } else {
                var result = getNextFromTable(table);
                stepToNextInTable(table);
                return new BaseEntry<MemorySegment>(result.get(0), result.get(1));
            }
        }

        if (table >= 0) {
            var tableResult = getNextFromTable(table);
            if (comparator.compare(tableResult.get(0), currentHashElem.key()) < 0) {
                stepToNextInTable(table);
                return new BaseEntry<MemorySegment>(tableResult.get(0), tableResult.get(1));
            } else if (comparator.compare(tableResult.get(0), currentHashElem.key()) == 0) {
                stepToNextInTable(table);
            }
            var result = currentHashElem;
            currentHashElem = null;
            return result;

        } else {
            var result = currentHashElem;
            currentHashElem = null;
            return result;
        }

    }
}
