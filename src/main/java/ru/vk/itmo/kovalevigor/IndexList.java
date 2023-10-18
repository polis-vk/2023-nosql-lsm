package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.AbstractList;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.SortedMap;

public class IndexList extends AbstractList<Entry<MemorySegment>> implements RandomAccess {

    public static final long INDEX_ENTRY_SIZE = 2 * ValueLayout.JAVA_LONG.byteSize();
    public static final long META_INFO_SIZE = ValueLayout.JAVA_LONG.byteSize();
    public static long MAX_BYTE_SIZE = META_INFO_SIZE + Integer.MAX_VALUE * INDEX_ENTRY_SIZE;

    private final MemorySegment indexSegment;
    private final MemorySegment dataSegment;
    private final long valuesOffset;

    public IndexList(final MemorySegment indexSegment, final MemorySegment dataSegment) {
        if (indexSegment.byteSize() > MAX_BYTE_SIZE) {
            this.indexSegment = indexSegment.asSlice(0, MAX_BYTE_SIZE);
        } else {
            this.indexSegment = indexSegment;
        }

        this.dataSegment = dataSegment;

        this.valuesOffset = readOffset(0);
    }

    private long getEntryOffset(final int index) {
        if (size() <= index) {
            return -1;
        }
        return META_INFO_SIZE + INDEX_ENTRY_SIZE * index;
    }

    private long readOffset(final long offset) {
        return indexSegment.get(ValueLayout.JAVA_LONG, offset);
    }

    public class LazyEntry implements Entry<MemorySegment> {

        private final MemorySegment key;
        private final int index;

        private LazyEntry(MemorySegment key, int index) {
            this.key = key;
            this.index = index;
        }

        @Override
        public MemorySegment key() {
            return key;
        }

        @Override
        public MemorySegment value() {
            return null;
        }

        public Entry<MemorySegment> fullRead() {
            return new BaseEntry<>(key, getValue(index));
        }
    }

    private class IndexIterator implements ListIterator<Entry<MemorySegment>> {

        private int cursor;

        public IndexIterator(final int cursor) {
            this.cursor = cursor;
        }

        @Override
        public boolean hasNext() {
            return cursor < size();
        }

        @Override
        public Entry<MemorySegment> next() {
            if (cursor >= size()) {
                throw new NoSuchElementException();
            }
            return get(cursor++).fullRead();
        }

        @Override
        public boolean hasPrevious() {
            return cursor > 0;
        }

        @Override
        public Entry<MemorySegment> previous() {
            if (cursor <= 0) {
                throw new NoSuchElementException();
            }
            return get(--cursor).fullRead();
        }

        @Override
        public int nextIndex() {
            return cursor;
        }

        @Override
        public int previousIndex() {
            return cursor - 1;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        @Override
        public void set(final Entry<MemorySegment> memorySegmentEntry) {
            throw new UnsupportedOperationException("set");
        }

        @Override
        public void add(final Entry<MemorySegment> memorySegmentEntry) {
            throw new UnsupportedOperationException("add");
        }
    }

    private MemorySegment getValue(final int index) {
        final long offset = getEntryOffset(index);
        final long valueOffset = readOffset(offset + ValueLayout.JAVA_LONG.byteSize());
        final long nextEntryOffset = getEntryOffset(index + 1);

        final MemorySegment value;
        if (valueOffset > 0) {
            long valueSize = (nextEntryOffset == -1
                    ? dataSegment.byteSize()
                    : Math.abs(readOffset(nextEntryOffset + ValueLayout.JAVA_LONG.byteSize()))) - valueOffset;
            value = dataSegment.asSlice(valueOffset, valueSize);
        } else {
            value = null;
        }
        return value;
    }

    @Override
    public LazyEntry get(final int index) {
        final long offset = getEntryOffset(index);

        final long keyOffset = readOffset(offset);
        final long nextEntryOffset = getEntryOffset(index + 1);

        long keySize = (nextEntryOffset == -1 ? valuesOffset : readOffset(nextEntryOffset)) - keyOffset;

        final MemorySegment key = dataSegment.asSlice(keyOffset, keySize);
        return new LazyEntry(key, index);
    }

    @Override
    public int size() {
        return (int)((indexSegment.byteSize() - META_INFO_SIZE) / INDEX_ENTRY_SIZE);
    }

    public static long getFileSize(final SortedMap<MemorySegment, Entry<MemorySegment>> map) {
        return META_INFO_SIZE + map.size() * INDEX_ENTRY_SIZE;
    }

    private static long writeLong(final MemorySegment writer, final long offset, final long value) {
        writer.set(ValueLayout.JAVA_LONG, offset, value);
        return offset + ValueLayout.JAVA_LONG.byteSize();
    }

    public static void write(
            final MemorySegment writer,
            final long[][] offsets,
            final long fileSize
    ) {

        long nextOffset = fileSize;
        for (int i = offsets.length - 1; i >= 0; i--) {
            if (offsets[i][1] == -1) {
                offsets[i][1] = -nextOffset;
            } else {
                nextOffset = offsets[i][1];
            }
        }

        long offset = writeLong(writer, 0, offsets.length > 0 ? Math.abs(offsets[0][1]) : 0);
        for (final long[] entry: offsets) {
            offset = writeLong(writer, offset, entry[0]);
            offset = writeLong(writer, offset, entry[1]);
        }
    }

    @Override
    public ListIterator<Entry<MemorySegment>> listIterator(final int index) {
        return new IndexIterator(index);
    }
}
