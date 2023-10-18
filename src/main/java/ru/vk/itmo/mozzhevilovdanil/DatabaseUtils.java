package ru.vk.itmo.mozzhevilovdanil;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;

public class DatabaseUtils {
    public static final Comparator<MemorySegment> comparator = DatabaseUtils::compare;

    static public Iterator<Entry<MemorySegment>> mergeIterator(Iterator<Entry<MemorySegment>> storageIterator, Iterator<Entry<MemorySegment>> sstableIterator) {
        return new Iterator<>() {
            Entry<MemorySegment> peekStorageEntry = null;
            Entry<MemorySegment> peekSSTableEntry = null;

            private static Entry<MemorySegment> peekNextIfNull(Iterator<Entry<MemorySegment>> iterator, Entry<MemorySegment> peekEntry) {
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
                return peekStorageEntry != null && peekSSTableEntry != null && comparator.compare(peekStorageEntry.key(), peekSSTableEntry.key()) == 0;
            }

            private boolean checkSSTablePeekLessThanStorage() {
                return peekSSTableEntry != null && comparator.compare(peekStorageEntry.key(), peekSSTableEntry.key()) > 0;
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
        };
    }

    static long binSearch(MemorySegment index, MemorySegment readPage, MemorySegment key) {
        long left = 0;
        long right = index.byteSize() / Long.BYTES;
        while (left < right) {
            long mid = (left + (right - left) / 2) * Long.BYTES;
            long offset = index.get(ValueLayout.JAVA_LONG_UNALIGNED, mid);
            long compareResult = compareInPlace(readPage, offset, key);
            if (compareResult == 0) {
                return mid;
            }
            if (compareResult < 0) {
                left = mid / Long.BYTES + 1;
            } else {
                right = mid / Long.BYTES;
            }
        }
        return left * Long.BYTES;
    }

    public static long compareInPlace(MemorySegment readPage, long offset, MemorySegment key){

        long keySize = readPage.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += 2 * Long.BYTES;

        long mismatch = MemorySegment.mismatch(readPage, offset, offset + key.byteSize(), key, 0, key.byteSize());

        if (mismatch == -1) {
            return Long.compare(keySize, key.byteSize());
        }

        if (mismatch == keySize) {
            return -1;
        }

        if (mismatch == key.byteSize()) {
            return 1;
        }
        byte b1 = readPage.get(ValueLayout.JAVA_BYTE, offset + mismatch);
        byte b2 = key.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    }

    private static int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        long mismatch = memorySegment1.mismatch(memorySegment2);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == memorySegment1.byteSize()) {
            return -1;
        }

        if (mismatch == memorySegment2.byteSize()) {
            return 1;
        }
        byte b1 = memorySegment1.get(ValueLayout.JAVA_BYTE, mismatch);
        byte b2 = memorySegment2.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    }
}