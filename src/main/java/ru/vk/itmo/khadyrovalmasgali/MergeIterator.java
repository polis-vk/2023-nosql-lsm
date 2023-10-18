package ru.vk.itmo.khadyrovalmasgali;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static ru.vk.itmo.khadyrovalmasgali.PersistentDao.comparator;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {

    private final ConcurrentSkipListMap<MemorySegment, MergeIteratorEntry> priorityMap;
    private final ArrayList<Iterator<Entry<MemorySegment>>> iters;
    private MergeIteratorEntry mentry = null;

    public MergeIterator(
            MemorySegment from,
            MemorySegment to,
            ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data,
            List<SSTable> sstables) {
        priorityMap = new ConcurrentSkipListMap<>(comparator);
        iters = new ArrayList<>();
        Iterator<Entry<MemorySegment>> inMemoryIt;
        if (from == null && to == null) {
            inMemoryIt = data.values().iterator();
        } else if (from == null) {
            inMemoryIt = data.headMap(to).values().iterator();
        } else if (to == null) {
            inMemoryIt = data.tailMap(from).values().iterator();
        } else {
            inMemoryIt = data.subMap(from, to).values().iterator();
        }
        inMemoryIt = new InMemoryIteratorWrapper(inMemoryIt);
        if (inMemoryIt.hasNext()) {
            Entry<MemorySegment> item = inMemoryIt.next();
            priorityMap.put(item.key(), new MergeIteratorEntry(item, 0));
        }
        iters.add(inMemoryIt);
        for (SSTable sstable : sstables) {
            Iterator<Entry<MemorySegment>> it = sstable.get(from, to);
            int index = iters.size();
            while (it.hasNext()) {
                Entry<MemorySegment> item = it.next();
                if (!priorityMap.containsKey(item.key())) {
                    priorityMap.put(item.key(), new MergeIteratorEntry(item, index));
                }
            }
            iters.add(it);
        }
        updateEntry();
    }

    private void updateEntry() {
        mentry = null;
        while (!priorityMap.isEmpty()) {
            MergeIteratorEntry item = priorityMap.pollFirstEntry().getValue();
            updateIter(iters.get(item.index), item.index);
            if (item.entry.value() != null) {
                mentry = item;
                break;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return mentry != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Merge Iterator has no elements left.");
        }
        Entry<MemorySegment> result = mentry.entry;
        Iterator<Entry<MemorySegment>> iter = iters.get(mentry.index);
        updateIter(iter, mentry.index);
        updateEntry();
        return result;
    }

    private void updateIter(Iterator<Entry<MemorySegment>> iter, int index) {
        while (iter.hasNext()) {
            Entry<MemorySegment> next = iter.next();
            if (priorityMap.containsKey(next.key())) {
                MergeIteratorEntry other = priorityMap.get(next.key());
                if (other.index < index) {
                    continue;
                } else {
                    updateIter(iters.get(other.index), other.index);
                }
            }
            priorityMap.put(next.key(), new MergeIteratorEntry(next, index));
            break;
        }
    }

    private static class MergeIteratorEntry {
        private final Entry<MemorySegment> entry;
        private final int index;

        public MergeIteratorEntry(Entry<MemorySegment> entry, int index) {
            this.entry = entry;
            this.index = index;
        }
    }

    private static class InMemoryIteratorWrapper implements Iterator<Entry<MemorySegment>> {

        private final Iterator<Entry<MemorySegment>> it;
        private Entry<MemorySegment> entry = null;

        public InMemoryIteratorWrapper(Iterator<Entry<MemorySegment>> it) {
            this.it = it;
            getNextEntry();
        }

        @Override
        public boolean hasNext() {
            return entry != null;
        }

        @Override
        public Entry<MemorySegment> next() {
            Entry<MemorySegment> result = entry;
            getNextEntry();
            return result;
        }

        private void getNextEntry() {
            if (it.hasNext()) {
                entry = it.next();
            } else {
                entry = null;
            }
        }
    }
}