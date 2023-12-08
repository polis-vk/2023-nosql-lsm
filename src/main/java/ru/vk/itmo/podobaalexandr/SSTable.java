package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Description:
 *  N - number of entries
 * Look of SSTable:
 *                      -------------------------------------
 *                         |offSetToKey_0||offsetToValue_0|
 *   indexes:                          .....
 *                      |offSetToKey_N-1||offsetToValue_N-1|
 *                      ------------------------------------
 *                               |key_0||value_0|
 *   data:                             .....
 *                             |key_N-1||value_N-1|
 *                      ------------------------------------
 */

public class SSTable implements Iterable<Entry<MemorySegment>> {

    private final MemorySegment page;
    private final long entriesCount;
    private final long size;

    public SSTable(Path file, Arena arena) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            size = Files.size(file);
            page = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
        }

        entriesCount = page.get(ValueLayout.JAVA_LONG_UNALIGNED, 0) / 2 / Long.BYTES;
    }

    public long getSize() {
        return size;
    }

    public Entry<MemorySegment> getEntryFromPage(MemorySegment keySearch) {
        long resultIndex = getIndexOfKeyFromPage(keySearch, false);
        return resultIndex == entriesCount ? null : getEntryFromIndex(resultIndex);
    }

    /** Binary Search index of entry in file.
     * @param keySearch - key that we find in SSTable
     * @param isBorder - if you find a border (from, to) set true, else false
     * @return      last index if isBorder = true.
     *              index of keySearch or count of entries if it is not presented in file.
     */
    private long getIndexOfKeyFromPage(MemorySegment keySearch, boolean isBorder) {

        long lo = 0;
        long hi = entriesCount - 1;

        int compare;

        while (lo <= hi) {
            long mid = (lo + hi) >>> 1;

            long keyOffset = getKeyOffset(mid);
            compare = MemorySegmentUtils.compareSegments(
                    keySearch,
                    keySearch.byteSize(),
                    page,
                    keyOffset,
                    keyOffset + getKeySize(mid)
            );

            if (compare == 0) {
                return mid;
            } else if (compare > 0) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        return isBorder ? lo : entriesCount;
    }

    private Entry<MemorySegment> getEntryFromIndex(long index) {
        MemorySegment key;
        MemorySegment value;

        long keyOffset = getKeyOffset(index);
        long valueOffset = getValueOffset(index);

        if (valueOffset == -1) {
            if (index == entriesCount - 1) {
                key = page.asSlice(keyOffset);
            } else {
                key = page.asSlice(keyOffset, getKeyOffset(index + 1) - keyOffset);
            }
            return new BaseEntry<>(key, null);
        }

        long keySize = valueOffset - keyOffset;
        key = page.asSlice(keyOffset, keySize);
        if (index == entriesCount - 1) {
            value = page.asSlice(valueOffset);
        } else {
            value = page.asSlice(valueOffset, getKeyOffset(index + 1) - valueOffset);
        }

        return new BaseEntry<>(key, value);
    }

    private long getKeyOffset(long index) {
        return page.get(ValueLayout.JAVA_LONG_UNALIGNED, index * 2 * Long.BYTES);
    }

    private long getValueOffset(long index) {
        return page.get(ValueLayout.JAVA_LONG_UNALIGNED, index * 2 * Long.BYTES + Long.BYTES);
    }

    private long getKeySize(long index) {
        long keyOffset = getKeyOffset(index);
        long valueOffset = getValueOffset(index);
        if (valueOffset == -1) {
            if (index == entriesCount - 1) {
                return page.byteSize() - keyOffset;
            }

            return getKeyOffset(index + 1) - keyOffset;
        }

        return valueOffset - keyOffset;
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return iterator(null, null);
    }

    public Iterator<Entry<MemorySegment>> iterator(MemorySegment from, MemorySegment to) {
        long fromIndex = from == null ? 0 : getIndexOfKeyFromPage(from, true);
        long toIndex = to == null ? entriesCount : getIndexOfKeyFromPage(to, true);

        return new Iterator<>() {

            long current = fromIndex;

            @Override
            public boolean hasNext() {
                return current < toIndex;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return getEntryFromIndex(current++);
            }
        };
    }

}
