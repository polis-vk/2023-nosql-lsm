package ru.vk.itmo.naumovivan;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;

public class InMemoryDaoIterator implements Iterator<Entry<MemorySegment>> {
    static private class LocalSSTableEntry {
        static int compareKeys(final LocalSSTableEntry e1,
                               final LocalSSTableEntry e2,
                               List<MemorySegment> indexPages,
                               final List<MemorySegment> dataPages) {
            return DaoUtils.compareSSTKeys(indexPages.get(e1.iSST), dataPages.get(e1.iSST), e1.offset,
                    indexPages.get(e2.iSST), dataPages.get(e2.iSST), e2.offset);
        }

        static Comparator<LocalSSTableEntry> getComparator(final List<MemorySegment> indexPages,
                                                           final List<MemorySegment> dataPages) {
            return (e1, e2) -> {
                final int cmp = compareKeys(e1, e2, indexPages, dataPages);
                return cmp != 0 ? cmp : Integer.compare(e1.iSST, e2.iSST);
            };
        }

        private long offset;
        final private int iSST;

        public LocalSSTableEntry(final int iSST, final long offset) {
            this.iSST = iSST;
            this.offset = offset;
        }

        public int getSSTIndex() {
            return iSST;
        }

        public long getOffset() {
            return offset;
        }

        public long setNextEntry() {
            offset += 2 * Long.BYTES;
            return offset;
        }

        public long readValueOffset(final List<MemorySegment> indexPages) {
            return indexPages.get(iSST).get(ValueLayout.JAVA_LONG_UNALIGNED, offset + Long.BYTES);
        }

        public Entry<MemorySegment> readEntry(final List<MemorySegment> indexPages, final List<MemorySegment> dataPages) {
            final MemorySegment indexPage = indexPages.get(iSST);
            final MemorySegment dataPage = dataPages.get(iSST);
            final long keyOffset = indexPage.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            final long valueOffset = Math.abs(indexPage.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + Long.BYTES));
            final long entryEndOffset = DaoUtils.getEntryEndOffset(indexPage, offset, dataPage);
            final MemorySegment key = dataPage.asSlice(keyOffset, valueOffset - keyOffset);
            final MemorySegment value = dataPage.asSlice(valueOffset, entryEndOffset - valueOffset);
            return new BaseEntry<>(key, value);
        }
    }

    final Iterator<Entry<MemorySegment>> memtableIterator;
    Entry<MemorySegment> memtableTopEntry;
    final List<MemorySegment> indexPages;
    final List<MemorySegment> dataPages;
    final List<Long> toIndexOffsets;
    final Queue<LocalSSTableEntry> queue;

    public InMemoryDaoIterator(final Iterator<Entry<MemorySegment>> memtableIterator,
                               final List<MemorySegment> indexPages,
                               final List<MemorySegment> dataPages,
                               final List<Long> from,
                               final List<Long> to) {
        this.memtableIterator = memtableIterator;
        this.indexPages = indexPages;
        this.dataPages = dataPages;
        this.toIndexOffsets = to;
        memtableTopEntry = memtableIterator.hasNext() ? memtableIterator.next() : null;
        queue = new PriorityQueue<>(LocalSSTableEntry.getComparator(indexPages, dataPages));
        for (int i = 0; i < indexPages.size(); ++i) {
            final long fromOffset = from.get(i);
            final long toOffset = toIndexOffsets.get(i);
            if (fromOffset < toOffset) {
                queue.add(new LocalSSTableEntry(i, fromOffset));
            }
        }
    }

    private int compareKeys(final Entry<MemorySegment> entry1, final LocalSSTableEntry entry2) {
        return DaoUtils.compareMSandSSTKey(entry1.key(),
                indexPages.get(entry2.getSSTIndex()),
                entry2.getOffset(),
                dataPages.get(entry2.getSSTIndex()));
    }

    private void moveWhileIterator() {
        LocalSSTableEntry head;
        while ((head = queue.peek()) != null && compareKeys(memtableTopEntry, head) == 0) {
            queue.poll();
            if (head.setNextEntry() != toIndexOffsets.get(head.getSSTIndex()))
                queue.add(head);
        }
    }

    private void moveWhileQueue(final LocalSSTableEntry head) {
        LocalSSTableEntry top;
        while ((top = queue.peek()) != null && LocalSSTableEntry.compareKeys(head, top, indexPages, dataPages) == 0) {
            queue.poll();
            if (top.setNextEntry() != toIndexOffsets.get(top.getSSTIndex()))
                queue.add(top);
        }
    }

    @Override
    public boolean hasNext() {
        if (queue.isEmpty()) {
            while (memtableTopEntry != null && memtableTopEntry.value() == null) {
                memtableTopEntry = memtableIterator.hasNext() ? memtableIterator.next() : null;
            }
            return memtableTopEntry != null;
        }
        LocalSSTableEntry head = queue.peek();
        if (memtableTopEntry == null) {
            while (head != null && head.readValueOffset(indexPages) < 0) {
                moveWhileQueue(head);
                head = queue.peek();
            }
            return head != null;
        }
        if (memtableTopEntry.value() != null || head.readValueOffset(indexPages) >= 0) {
            return true;
        }
        if (compareKeys(memtableTopEntry, head) <= 0) {
            moveWhileIterator();
            memtableTopEntry = memtableIterator.hasNext() ? memtableIterator.next() : null;
        } else {
            moveWhileQueue(head);
        }
        return hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        if (queue.isEmpty()) {
            Entry<MemorySegment> tmp = memtableTopEntry;
            memtableTopEntry = memtableIterator.hasNext() ? memtableIterator.next() : null;
            return tmp;
        }
        if (memtableTopEntry == null) {
            final LocalSSTableEntry head = queue.poll();
            final Entry<MemorySegment> entry = head.readEntry(indexPages, dataPages);
            if (head.setNextEntry() != toIndexOffsets.get(head.getSSTIndex())) {
                queue.add(head);
            }
            return entry;
        }

        LocalSSTableEntry head = queue.peek();
        if (compareKeys(memtableTopEntry, head) <= 0) {
            moveWhileIterator();
            Entry<MemorySegment> tmp = memtableTopEntry;
            memtableTopEntry = memtableIterator.hasNext() ? memtableIterator.next() : null;
            return tmp.value() == null ? next() : tmp;
        } else {
            moveWhileQueue(head);
            if (head.readValueOffset(indexPages) < 0) {
                return next();
            }
            return head.readEntry(indexPages, dataPages);
        }
    }
}
