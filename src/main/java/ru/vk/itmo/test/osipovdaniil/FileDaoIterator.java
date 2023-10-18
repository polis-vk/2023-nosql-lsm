package ru.vk.itmo.test.osipovdaniil;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileDaoIterator implements SavingIterator {

    final MemorySegment fileMemorySegment;
    final MemorySegment from, to;
    long currOffsetOffset, toOffsetOffset;

    final long entriesOffset;

    private Entry<MemorySegment> currEntry;

    public FileDaoIterator(final MemorySegment fileMemorySegment,
                           final MemorySegment from,
                           final MemorySegment to) {
        this.fileMemorySegment = fileMemorySegment;
        this.from = from;
        this.to = to;
        this.entriesOffset = Long.BYTES + Long.BYTES * 2
                * fileMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);

        this.currOffsetOffset = from == null ? Long.BYTES : findKeyOffsetOffset(from);
        this.toOffsetOffset = to == null ? entriesOffset - 2 * Long.BYTES : findKeyOffsetOffset(to);
        currEntry = getEntryAtKeyOffsetOffset(currOffsetOffset);
    }

    private long findKeyOffsetOffset(final MemorySegment key) {
        long minOffsetOffset = Long.BYTES;
        long maxOffsetOffset = entriesOffset;
        while (maxOffsetOffset - minOffsetOffset > Long.BYTES * 2) {
            long mOffsetOffset = (maxOffsetOffset + minOffsetOffset) / 2;
            long keySegOff = fileMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, mOffsetOffset);
            long valSegOff = fileMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, mOffsetOffset + Long.BYTES);
            if (Utils.compareMemorySegments(key,
                    fileMemorySegment.asSlice(keySegOff, valSegOff - keySegOff)) < 0) {
                maxOffsetOffset = mOffsetOffset;
            } else {
                minOffsetOffset = mOffsetOffset;
            }
        }
        return minOffsetOffset;
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        return currEntry != null;
    }


    Entry<MemorySegment> getEntryAtKeyOffsetOffset(final long keyOffsetOffset) {
        if (keyOffsetOffset == entriesOffset) {
            return null;
        }
        final long keyOffset = fileMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffsetOffset);
        final long valueOffset = fileMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                keyOffsetOffset + Long.BYTES);
        final long nextKeyOffset = (keyOffsetOffset + 2 * Long.BYTES == entriesOffset) ?
                fileMemorySegment.byteSize() : fileMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                currOffsetOffset + 2 * Long.BYTES);
        final MemorySegment key = fileMemorySegment.asSlice(keyOffset, valueOffset - keyOffset);
        final MemorySegment value = fileMemorySegment.asSlice(valueOffset, nextKeyOffset - valueOffset);
        return new BaseEntry<>(key, value);
    }


    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final Entry<MemorySegment> res = currEntry;
        currOffsetOffset += 2 * Long.BYTES;
        currEntry = currOffsetOffset < toOffsetOffset ? getEntryAtKeyOffsetOffset(currOffsetOffset) : null;
        return res;
    }

    @Override
    public Entry<MemorySegment> getCurrEntry() {
        return currEntry;
    }
}
