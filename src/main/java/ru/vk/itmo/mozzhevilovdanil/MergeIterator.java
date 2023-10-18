package ru.vk.itmo.mozzhevilovdanil;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

import static ru.vk.itmo.mozzhevilovdanil.DatabaseUtils.comparator;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {

    Entry<MemorySegment> peekStorageEntry;
    Entry<MemorySegment> peekSSTableEntry;

    Iterator<Entry<MemorySegment>> storageIterator;
    Iterator<Entry<MemorySegment>> sstableIterator;

    MergeIterator(
            Iterator<Entry<MemorySegment>> storageIterator,
            Iterator<Entry<MemorySegment>> sstableIterator
    ) {
        this.storageIterator = storageIterator;
        this.sstableIterator = sstableIterator;
    }

    private static Entry<MemorySegment> peekNextIfNull(
            Iterator<Entry<MemorySegment>> iterator,
            Entry<MemorySegment> peekEntry
    ) {
        if (peekEntry == null && iterator.hasNext()) {
            return iterator.next();
        }
        return peekEntry;
    }

    @Override
    public boolean hasNext() {
        peekSSTableEntry = peekNextIfNull(sstableIterator, peekSSTableEntry);
        peekStorageEntry = peekNextIfNull(storageIterator, peekStorageEntry);
        if (checkSameKeysOnPeek()) {
            peekSSTableEntry = null;
        }
        if (checkNullOnStoragePeek()) {
            if (checkSSTablePeekLessThanStorage()) {
                return peekStorageEntry != null || peekSSTableEntry != null;
            }
            peekStorageEntry = null;
            return hasNext();
        }

        return peekStorageEntry != null || peekSSTableEntry != null;
    }

    private boolean checkNullOnStoragePeek() {
        return peekStorageEntry != null && peekStorageEntry.value() == null;
    }

    private boolean checkSameKeysOnPeek() {
        return peekStorageEntry != null && peekSSTableEntry != null
                && comparator.compare(peekStorageEntry.key(), peekSSTableEntry.key()) == 0;
    }

    private boolean checkSSTablePeekLessThanStorage() {
        return peekSSTableEntry != null
                && comparator.compare(peekStorageEntry.key(), peekSSTableEntry.key()) > 0;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            return null;
        }
        if (peekStorageEntry == null) {
            Entry<MemorySegment> result = peekSSTableEntry;
            peekSSTableEntry = null;
            return result;
        }
        if (peekSSTableEntry == null) {
            Entry<MemorySegment> result = peekStorageEntry;
            peekStorageEntry = null;
            return result;
        }
        int compare = comparator.compare(peekStorageEntry.key(), peekSSTableEntry.key());
        if (compare < 0) {
            Entry<MemorySegment> result = peekStorageEntry;
            peekStorageEntry = null;
            return result;
        }
        if (compare > 0) {
            Entry<MemorySegment> result = peekSSTableEntry;
            peekSSTableEntry = null;
            return result;
        }
        Entry<MemorySegment> result = peekStorageEntry;
        peekStorageEntry = null;
        peekSSTableEntry = null;
        return result;
    }
}
