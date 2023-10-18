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

    private final int[] storagePositions;
    private final int[] entryNumber;

    Entry<MemorySegment> nextEntry;

    boolean isNullValueWasFound = false;

    private final ArrayList<Entry<MemorySegment>> cache;
    private int lastIndex = -1;

    public InMemoryIterator(Iterator<Entry<MemorySegment>> memTableIterator,
                            int ssTablesCount,
                            MemorySegment from,
                            MemorySegment to) throws IOException {

        this.memTableIterator = memTableIterator;
        this.ssTablesCount = ssTablesCount;
        this.from = from;
        this.to = to;

        // Последний элемент для MemTable.
        storagePositions = new int[ssTablesCount + 1];
        entryNumber = new int[ssTablesCount];
        cache = new ArrayList<>(ssTablesCount + 1);

        for (int i = 0; i <= ssTablesCount; i++) {
            cache.add(null);
        }

        initState();

        nextEntry = getNextFromStorage();
    }

    @Override
    public boolean hasNext() {

        return nextEntry != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Entry<MemorySegment> returnValue = nextEntry;
        nextEntry = getNextFromStorage();
        return returnValue;
    }

    private Entry<MemorySegment> getNextFromStorage() {

        while (true) {

            Entry<MemorySegment> minEntry = null;
            int savedPosition = -1;
            for (int i = ssTablesCount; i >= 0; i--) {

                if (storagePositions[i] == -1) {
                    continue;
                }
                if (storagePositions[i] == 4) {
                    System.out.println();
                }

                Entry<MemorySegment> currEntry = (i == ssTablesCount) ? getNextFromMemTable() : getFromStorage(i);
                if (currEntry == null) {
                    continue;
                }

                if (minEntry != null) {

                    long compareResult = segmentComparator.compare(currEntry.key(), minEntry.key());
                    if (compareResult < 0) {
                        minEntry = currEntry;
                        savedPosition = i;
                    } else if (compareResult == 0) { // Entry по такому же ключу уже неактуальны.
                        incrementStoragePosition(i);
                        cache.set(i, null);
                    }
                } else {
                    minEntry = currEntry;
                    savedPosition = i;
                }
            }

            if (savedPosition >= 0 && savedPosition <= ssTablesCount) {
                incrementStoragePosition(savedPosition);
                lastIndex = savedPosition;
            }

            if (minEntry == null || minEntry.value() != null) {
                return minEntry;
            }
        }
    }

    private void incrementStoragePosition(int index) {
        if (storagePositions[index] >= 0) {
            storagePositions[index]++;
        }
    }

    private Entry<MemorySegment> getFromStorage(int index) {

        // Берем значение из кэша, если в предыдущий раз не было взято.
        if (lastIndex != index) {
            Entry<MemorySegment> entry = cache.get(index);
            if (entry != null) {
                return entry;
            }
        }

        int storagePos = storagePositions[index];
        MemorySegment meta = metaTables[index];
        MemorySegment ssTable = ssTables[index];

        int keyOffset = getKeyOffsetByIndex(meta, storagePos);
        int keySize = getKeySize(ssTable, meta, storagePos, keyOffset);
        MemorySegment key = ssTable.asSlice(keyOffset, keySize);

        if (to != null) {
            long mismatchTo = segmentComparator.compare(key, to);
            if (mismatchTo >= 0) {
                storagePositions[index] = -1;
                return null;
            }
        }

        MemorySegment value = isNullValue(meta, storagePos) ? null : getValueSegment(ssTable, meta, storagePos);
        Entry<MemorySegment> entry = new BaseEntry<>(key, value);

        // Кэшируем последний выбранный элемент.
        cache.set(index, entry);

        // Помечаем следует ли в следующий раз искать по этой ssTable.
        if (storagePos == entryNumber[index] - 1) {
            storagePositions[index] = -1;
        }

        return entry;
    }

    private void initState() throws IOException {

        // init ssTables state
        for (int i = ssTablesCount - 1; i >= 0; i--) {
            if (ssTables[i] == null) {
                createStorageSegment(i);
            }
            MemorySegment ssTable = ssTables[i];
            MemorySegment meta = metaTables[i];

            entryNumber[i] = meta.get(ValueLayout.JAVA_INT_UNALIGNED, 0);

            int pos = (from == null) ? 0 : findKeyPositionOrNearest(ssTable, meta, from);
            MemorySegment key = getKeySegment(ssTable, meta, pos);

            storagePositions[i] = (isFitsInInterval(key) > 0) ? -1 : pos;
        }

        // init memTable state
        while (memTableIterator.hasNext()) {
            Entry<MemorySegment> next = memTableIterator.next();

            int isFits = isFitsInInterval(next.key());
            if (isFits == 0) {
                cache.set(ssTablesCount, next);
                break;
            } else if (isFits > 0) {
                storagePositions[ssTablesCount] = -1;
            }
        }
    }

    private Entry<MemorySegment> getNextFromMemTable() {
        if (lastIndex != ssTablesCount) {
            return cache.get(ssTablesCount);
        }

        boolean hasNext = memTableIterator.hasNext();

        if (!hasNext) {
            return null;
        }

        Entry<MemorySegment> next = memTableIterator.next();

        // Помечаем следует ли в следующий раз искать по memTable.
        if (!memTableIterator.hasNext()) {
            storagePositions[ssTablesCount] = -1;
        }

        if (isFitsInInterval(next.key()) == 0) {
            cache.set(ssTablesCount, next);
            return next;
        } else {
            storagePositions[ssTablesCount] = -1;
            return null;
        }
    }

    private int isFitsInInterval(MemorySegment key) {
        if (from != null) {
            long mismatch = segmentComparator.compare(key, from);
            if (mismatch < 0) {
                return -1;
            }
        }
        if (to != null) {
            long mismatchTo = segmentComparator.compare(key, to);
            if (mismatchTo > 0) {
                return 1;
            }
        }
        return 0;
    }
}
