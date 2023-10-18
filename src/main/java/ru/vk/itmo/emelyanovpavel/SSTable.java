package ru.vk.itmo.emelyanovpavel;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

public class SSTable {
    private final MemorySegment mappedData;
    private final MemorySegment mappedIndex;
    private final int priority;

    public SSTable(MemorySegment mappedData, MemorySegment mappedIndex, int priority) {
        this.mappedData = mappedData;
        this.mappedIndex = mappedIndex;
        this.priority = priority;
    }

    public PeekIterator<Entry<MemorySegment>> iterator(MemorySegment from, MemorySegment to) {
        return new PeekIteratorImpl(new SSTableIterator(from, to), priority);
    }

    private class SSTableIterator implements Iterator<Entry<MemorySegment>> {
        private final MemorySegment to;
        private long indexOffset;
        private long currentKeyOffset = -1;
        private long currentKeySize = -1;

        SSTableIterator(MemorySegment from, MemorySegment to) {
            indexOffset = from == null ? 0 : binarySearch(from);
            this.to = to;
        }

        @Override
        public boolean hasNext() {
            if (indexOffset == mappedIndex.byteSize()) {
                return false;
            }
            if (to == null) {
                return true;
            }
            currentKeyOffset = mappedIndex.get(JAVA_LONG_UNALIGNED, indexOffset);
            currentKeySize = mappedData.get(JAVA_LONG_UNALIGNED, currentKeyOffset);

            long fromOffset = currentKeyOffset + Long.BYTES;
            return compare(to, mappedData, fromOffset, fromOffset + currentKeySize) > 0;
        }

        @Override
        public Entry<MemorySegment> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            long keyOffset = getCurrentKeyOffset();
            long keySize = getCurrentKeySize(keyOffset);

            indexOffset += Long.BYTES;
            keyOffset += Long.BYTES;
            MemorySegment key = mappedData.asSlice(keyOffset, keySize);
            keyOffset += keySize;

            long valueSize = mappedData.get(JAVA_LONG_UNALIGNED, keyOffset);
            MemorySegment value = valueSize == -1 ? null : mappedData.asSlice(keyOffset + Long.BYTES, valueSize);
            return new BaseEntry<>(key, value);
        }

        private long getCurrentKeyOffset() {
            return currentKeyOffset == -1 ? mappedIndex.get(JAVA_LONG_UNALIGNED, indexOffset) : currentKeyOffset;
        }

        private long getCurrentKeySize(long keyOffset) {
            return currentKeySize == -1 ? mappedData.get(JAVA_LONG_UNALIGNED, keyOffset) : currentKeySize;
        }

        private long binarySearch(MemorySegment key) {
            long left = 0;
            long right = getRightLimit();

            while (left <= right) {
                long mid = ((right - left) >>> 1) + left;

                long offset = mappedIndex.get(JAVA_LONG_UNALIGNED, mid * Long.BYTES);

                long currentSize = mappedData.get(JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                int comparator = compare(key, mappedData, offset, offset + currentSize);
                if (comparator == 0) {
                    return mid * Long.BYTES;
                }

                if (comparator > 0) {
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            }
            return left * Long.BYTES;
        }

        private long getRightLimit() {
            return mappedIndex.byteSize() / Long.BYTES - 1;
        }

        private static int compare(MemorySegment segment1, MemorySegment segment2, long fromOffset, long toOffset) {
            long mismatchOffset =
                    MemorySegment.mismatch(
                            segment1, 0, segment1.byteSize(),
                            segment2, fromOffset, toOffset
                    );
            if (mismatchOffset == -1) {
                return 0;
            }
            if (mismatchOffset == segment1.byteSize()) {
                return -1;
            }
            if (mismatchOffset == segment2.byteSize()) {
                return 1;
            }
            byte first = segment1.get(JAVA_BYTE, mismatchOffset);
            byte second = segment2.get(JAVA_BYTE, fromOffset + mismatchOffset);
            return Byte.compare(first, second);
        }
    }
}
