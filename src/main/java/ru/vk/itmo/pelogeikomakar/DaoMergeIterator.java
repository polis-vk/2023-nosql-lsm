package ru.vk.itmo.pelogeikomakar;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DaoMergeIterator implements Iterator<Entry<MemorySegment>> {

    private final MemorySegmentComparator comparator = new MemorySegmentComparator();
    private final MemorySegment from;
    private final MemorySegment to;
    private final Iterator<Entry<MemorySegment>> hashMapIter;
    private Entry<MemorySegment> currentHashElem;
    private Entry<MemorySegment> readyNextEntry;
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

    private Triple goAcrossTables(MemorySegment minKey, int minTableIndex) {
        long minSizeOfVal = -1;
        for (int i = 0; i < indexList.size(); ++i) {
            indexOffsetMap.putIfAbsent(i, getStartOffset(i, from));

            if (indexOffsetMap.get(i) < 0) {
                continue;
            }
            long dataOffset = indexList.get(i).get(ValueLayout.JAVA_LONG_UNALIGNED, indexOffsetMap.get(i));
            long sizeOfKey = tableList.get(i).get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
            MemorySegment currKey = tableList.get(i).asSlice(dataOffset + Long.BYTES, sizeOfKey);

            var table = tableList.get(i);
            long sizeOfVal = table.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset + Long.BYTES + sizeOfKey);

            if (to != null && comparator.compare(currKey, to) >= 0) {
                continue;
            }
            if (minKey == null || comparator.compare(minKey, currKey) > 0) {
                minTableIndex = i;
                minKey = currKey;
                minSizeOfVal = sizeOfVal;
            } else if (comparator.compare(minKey, currKey) == 0) {
                stepToNextInTable(i);
            }
        }

        return new Triple(minKey, minTableIndex, minSizeOfVal);
    }

    private int getNextTable() {

        int minTableIndex = -1;
        MemorySegment minKey;
        long minSizeOfVal = -1;
        boolean hasNext = true;

        while (minSizeOfVal == -1 && hasNext) {
            minTableIndex = -1;
            minKey = null;

            var singleWlk = goAcrossTables(minKey, minTableIndex);
            minKey = (MemorySegment) singleWlk.getFirst();
            minTableIndex = (int) singleWlk.getSecond();
            minSizeOfVal = (long) singleWlk.getThird();

            if (minKey == null) {
                minTableIndex = -1;
                hasNext = false;
            }
            if (minSizeOfVal == -1 && minTableIndex >= 0) {
                stepToNextInTable(minTableIndex);
            }
        }
        return minTableIndex;
    }

    private long getStartOffset(int table, MemorySegment key) {
        if (from == null) {
            return 0L;
        }

        int comp;
        comp = 1;
        long low;
        long high;
        low = 0;
        var indexCurr = indexList.get(table);
        var tableCurr = tableList.get(table);
        high = (indexCurr.byteSize() - 1) / Long.BYTES;
        long mid;
        mid = -1000;
        long dataOffset;
        long sizeOfKey;
        MemorySegment currKey;

        while (low <= high) {
            mid = low + ((high - low) / 2);

            dataOffset = indexCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, mid * Long.BYTES);
            sizeOfKey = tableCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
            currKey = tableCurr.asSlice(dataOffset + Long.BYTES, sizeOfKey);

            comp = comparator.compare(currKey, key);

            if (low == high) {
                if (comp == 0) {
                    return mid * Long.BYTES;
                }
                break;
            }

            if (comp < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        // Chose one of 3 variants if we are in the middle
        long left = mid - 1 >= 0 ? high - 1 : 0;
        dataOffset = indexCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, left * Long.BYTES);
        sizeOfKey = tableCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
        currKey = tableCurr.asSlice(dataOffset + Long.BYTES, sizeOfKey);
        if (comparator.compare(currKey, key) >= 0) {
            return left * Long.BYTES;
        }
        if (mid != -1000 && comp >= 0) {
            return mid * Long.BYTES;
        }
        long right = Math.min(mid + 1, ((indexCurr.byteSize() - 1) / Long.BYTES));
        dataOffset = indexCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, right * Long.BYTES);
        sizeOfKey = tableCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
        currKey = tableCurr.asSlice(dataOffset + Long.BYTES, sizeOfKey);
        if (comparator.compare(currKey, key) >= 0) {
            return right * Long.BYTES;
        }
        return -1L;
    }

    private List<MemorySegment> getNextFromTable(int tableIdx) {
        long dataOffset = indexList.get(tableIdx).get(ValueLayout.JAVA_LONG_UNALIGNED, indexOffsetMap.get(tableIdx));
        var table = tableList.get(tableIdx);
        long sizeOfKey = table.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
        long sizeOfVal = table.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset + Long.BYTES + sizeOfKey);
        long valOffset = dataOffset + 2L * Long.BYTES + sizeOfKey;
        MemorySegment key = table.asSlice(dataOffset + Long.BYTES, sizeOfKey);
        MemorySegment value = sizeOfVal == -1 ? null : table.asSlice(valOffset, sizeOfVal);
        List<MemorySegment> result = new ArrayList<>();
        result.add(key);
        result.add(value);

        return result;
    }

    private void stepToNextInTable(int tableIdx) {
        if (indexOffsetMap.get(tableIdx) == -1L) {
            return;
        }
        indexOffsetMap.put(tableIdx, indexOffsetMap.get(tableIdx) + Long.BYTES);

        if (indexOffsetMap.get(tableIdx) == indexList.get(tableIdx).byteSize()) {
            indexOffsetMap.put(tableIdx, -1L);
        }
    }

    private Entry<MemorySegment> nextPopTabled(boolean pop, int table) {
        var tableResult = getNextFromTable(table);
        if (comparator.compare(tableResult.get(0), currentHashElem.key()) < 0) {
            if (pop) {
                stepToNextInTable(table);
            }
            return new BaseEntry<MemorySegment>(tableResult.get(0), tableResult.get(1));
        } else if (comparator.compare(tableResult.get(0), currentHashElem.key()) == 0 && pop) {
            stepToNextInTable(table);
        }
        var result = currentHashElem;
        currentHashElem = null;
        return result;
    }

    private Entry<MemorySegment> nextPopOption(boolean pop) {
        if (!hasNextNullable()) {
            return null;
        }
        int table = getNextTable();

        if (currentHashElem == null) {
            if (hashMapIter.hasNext()) {
                currentHashElem = hashMapIter.next();
            } else {
                var result = getNextFromTable(table);
                if (pop) {
                    stepToNextInTable(table);
                }
                return new BaseEntry<MemorySegment>(result.get(0), result.get(1));
            }
        }

        if (table >= 0) {
            return nextPopTabled(pop, table);
        } else {
            var result = currentHashElem;
            currentHashElem = null;
            return result;
        }
    }

    public boolean hasNextNullable() {
        if (hashMapIter.hasNext()) {
            return true;
        }

        if (currentHashElem != null) {
            return true;
        }

        return getNextTable() != -1;
    }

    @Override
    public boolean hasNext() {
        Entry<MemorySegment> entry;
        do {
            entry = nextPopOption(true);
            if (entry == null) {
                return false;
            }
        } while (entry.value() == null);

        readyNextEntry = entry;
        return true;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (readyNextEntry == null) {
            Entry<MemorySegment> entry;
            do {
                entry = nextPopOption(true);
                if (entry == null) {
                    return null;
                }
            } while (entry.value() == null);
            return entry;
        } else {
            var result = readyNextEntry;
            readyNextEntry = null;
            return result;
        }
    }
}
