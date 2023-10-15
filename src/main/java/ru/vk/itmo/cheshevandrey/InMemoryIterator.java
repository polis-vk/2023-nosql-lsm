package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.*;

import static ru.vk.itmo.cheshevandrey.InMemoryDao.*;

public class InMemoryIterator implements Iterator<Entry<MemorySegment>> {

    private final Iterator<Entry<MemorySegment>> memTableIterator;

    private final int ssTablesCount;

    private final MemorySegment from;
    private final MemorySegment to;

    private final Entry<MemorySegment>[][] cache;
    private final int[] positions;

    private int loadedElementsCount;

    private static final int CACHE_CAPACITY = 1024;

    public InMemoryIterator(Iterator<Entry<MemorySegment>> memTableIterator,
                            int ssTablesCount,
                            MemorySegment from,
                            MemorySegment to) throws IOException {

        this.memTableIterator = memTableIterator;
        this.ssTablesCount = ssTablesCount;
        this.from = from;
        this.to = to;

        cache = new Entry[ssTablesCount + 1][CACHE_CAPACITY];
        positions = new int[ssTablesCount + 1];

        initStartState();
    }

    @Override
    public boolean hasNext() {
        if (loadedElementsCount == 0) {
            collectData();
        }

        return loadedElementsCount != 0;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Entry<MemorySegment> minEntry = null;
        int savedPosition = -1;
        for (int i = ssTablesCount; i >= 0; i--) {

            int position = positions[i];
            if (position == -1) {
                continue;
            }

            Entry<MemorySegment> currEntry = cache[i][position];
            if (currEntry == null) {
                positions[i] = -1;
                continue;
            }

            if (minEntry != null) {
                long compareResult = segmentComparator.compare(currEntry.key(), minEntry.key());
                if (compareResult < 0) {
                    minEntry = currEntry;
                    savedPosition = i;
                } else if (compareResult == 0) {
                    positions[i]++;
                }
            } else {
                minEntry = currEntry;
                savedPosition = i;
            }
        }

        if (savedPosition >= 0) {
            positions[savedPosition]++;
            loadedElementsCount--;
        }
        return minEntry;
    }

    private void collectData() {
        collectFromMemTable();
        collectFromStorage();
    }

    private void collectFromMemTable() {

        Entry<MemorySegment> currMemTableEntry;
        int index = 0;
        boolean shouldSavePosition = true;
        while (index < CACHE_CAPACITY) {
            if (memTableIterator.hasNext()) {
                currMemTableEntry = memTableIterator.next();
            } else {
                break;
            }

            long mismatchFrom = segmentComparator.compare(currMemTableEntry.key(), from);
            long mismatchTo = segmentComparator.compare(currMemTableEntry.key(), to);
            if (mismatchFrom < 0) {
                continue;
            }
            if (mismatchTo > 0) {
                break;
            }

            cache[ssTablesCount][index] = currMemTableEntry;

            if (shouldSavePosition) {
                positions[ssTablesCount] = index;
                shouldSavePosition = false;
            }

            loadedElementsCount++;
            index++;
        }

        if (index < CACHE_CAPACITY - 1) {
            cache[ssTablesCount][index + 1] = null;
        }
    }

    private void collectFromStorage() {

    }

    private void initStartState() throws IOException {
        for (int i = ssTablesCount; i > 0; i--) {
            if (ssTables[i] == null) {
                createStorageSegment(i);
            }
            MemorySegment ssTable = ssTables[i];
            MemorySegment meta = metaTables[i];

            int pos = findKeyPositionOrNearest(ssTable, meta, from);

            int keyOffset = getKeyOffsetByIndex(meta, pos);
            int keySize = getKeySize(meta, pos, keyOffset);

            /// ??? not optimal
            MemorySegment key = ssTable.asSlice(keyOffset, keySize);
            int compareResult = segmentComparator.compare(key, to);

            if (compareResult > 0) {
                positions[i] = -1;
            } else {
                positions[i] = pos;
            }
        }

    }
}
