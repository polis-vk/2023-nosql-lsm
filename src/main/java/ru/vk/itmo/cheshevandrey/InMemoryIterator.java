package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;

import static ru.vk.itmo.cheshevandrey.InMemoryDao.*;

public class InMemoryIterator implements Iterator<Entry<MemorySegment>> {

    private final Iterator<Entry<MemorySegment>> memTableIterator;

    private final int ssTablesCount;

    private final MemorySegment from;
    private final MemorySegment to;

    private final Entry<MemorySegment>[][] cache;
    private final int[] storagePositions;
    private final int[] cachePositions;

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
        storagePositions = new int[ssTablesCount];
        cachePositions = new int[ssTablesCount + 1];

        initState();
    }

    @Override
    public boolean hasNext() {
        if (loadedElementsCount == 0) {
            fillCache(0);
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

            int position = cachePositions[i];

            if (position == CACHE_CAPACITY) {
                if (i == ssTablesCount) {
                    fillCache(1);
                } else {
                    fillCache(-1);
                }
            }

            Entry<MemorySegment> currEntry = cache[i][position];
            if (currEntry == null) {
                continue;
            }

            if (minEntry != null) {
                long compareResult = segmentComparator.compare(currEntry.key(), minEntry.key());
                if (compareResult < 0) {
                    minEntry = currEntry;
                    savedPosition = i;
                } else if (compareResult == 0) { // Entry по такому же ключу уже неактуальны.
                    cachePositions[i]++;
                    loadedElementsCount--;
                }
            } else {
                minEntry = currEntry;
                savedPosition = i;
            }
        }

        if (savedPosition >= 0) {
            cachePositions[savedPosition]++;
            loadedElementsCount--;
        }
        return minEntry;
    }

    private void fillCache(int condition) {
        if (condition >= 0) {
            fillCacheFromMemTable();
        }
        if (condition <= 0) {
            for (int index = ssTablesCount - 1; index >= 0; index--) {
                fillCacheFromStorage(index);
            }
        }
    }

    private void fillCacheFromMemTable() {

        int index = 0;
        while (index < CACHE_CAPACITY) {

            Entry<MemorySegment> currMemTableEntry;
            if (memTableIterator.hasNext()) {
                currMemTableEntry = memTableIterator.next();
            } else {
                break;
            }

            int isFits = isFitsInInterval(currMemTableEntry.key());
            if (isFits > 0) {
                continue;
            } else if (isFits < 0) {
                break;
            }

            cache[ssTablesCount][index] = currMemTableEntry;
            loadedElementsCount++;
            index++;
        }

        cachePositions[ssTablesCount] = (index > 0) ? 0 : -1;

        if (index < CACHE_CAPACITY - 1) {
            cache[ssTablesCount][index + 1] = null;
        }
    }

    private void fillCacheFromStorage(int index) {

        int storagePos = storagePositions[index];
        if (storagePos == -1) {
            return;
        }

        MemorySegment ssTable = ssTables[index];
        MemorySegment meta = metaTables[index];
        int entryNumber = meta.get(ValueLayout.JAVA_INT_UNALIGNED, 0);

        int cachePos = 0;
        while (cachePos < CACHE_CAPACITY && storagePos < entryNumber) {

            int keyOffset = getKeyOffsetByIndex(meta, storagePos);
            int keySize = getKeySize(meta, storagePos, keyOffset);

            MemorySegment key = ssTable.asSlice(keyOffset, keySize);

            int isFits = isFitsInInterval(key);
            if (isFits > 0) {
                storagePos++;
                continue;
            } else if (isFits < 0) {
                break;
            }

            MemorySegment value = getValueSegment(ssTable, meta, storagePos);
            cache[index][cachePos] = new BaseEntry<>(key, value);

            loadedElementsCount++;
            storagePos++;
            cachePos++;
        }

        cachePositions[index] = (index > 0) ? 0 : -1;
    }

    private int isFitsInInterval(MemorySegment key) {
        if (from != null) {
            long mismatch = segmentComparator.compare(key, from);
            if (mismatch < 0) {
                return 1;
            }
        }
        if (to != null) {
            long mismatchTo = segmentComparator.compare(key, to);
            if (mismatchTo > 0) {
                return -1;
            }
        }
        return 0;
    }

    private void initState() throws IOException {
        for (int i = ssTablesCount - 1; i >= 0; i--) {
            if (ssTables[i] == null) {
                createStorageSegment(i);
            }
            MemorySegment ssTable = ssTables[i];
            MemorySegment meta = metaTables[i];

            int pos = (from == null) ? 0 : findKeyPositionOrNearest(ssTable, meta, from);
            MemorySegment key = getKeySegment(ssTable, meta, pos);

            storagePositions[i] = (isFitsInInterval(key) < 0) ? -1 : pos;
        }
    }
}
