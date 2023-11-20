package ru.vk.itmo.khadyrovalmasgali;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {

    private final SortedMap<MemorySegment, MergeIteratorEntry> priorityMap;
    private final List<Iterator<Entry<MemorySegment>>> iters;
    private MergeIteratorEntry mentry;

    public MergeIterator(
            MemorySegment from,
            MemorySegment to,
            ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data,
            List<SSTable> sstables,
            Comparator<MemorySegment> comparator) {
        priorityMap = new TreeMap<>(comparator);
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
        while (!priorityMap.isEmpty() && mentry == null) {
            MergeIteratorEntry item = priorityMap.pollFirstEntry().getValue();
            updateIter(iters.get(item.index), item.index);
            if (item.entry.value() != null) {
                mentry = item;
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
        updateEntry();
        return result;
    }

    private void updateIter(Iterator<Entry<MemorySegment>> it, int index) {
        Queue<Iterator<Entry<MemorySegment>>> q = new ArrayDeque<>();
        q.add(it);
        while (!q.isEmpty()) {
            Iterator<Entry<MemorySegment>> curIter = q.poll();
            boolean flag = curIter.hasNext();
            while (flag) {
                Entry<MemorySegment> next = curIter.next();
                flag = curIter.hasNext();
                if (priorityMap.containsKey(next.key())) {
                    MergeIteratorEntry other = priorityMap.get(next.key());
                    if (other.index < index) {
                        continue;
                    } else {
                        q.add(iters.get(other.index));
                    }
                }
                priorityMap.put(next.key(), new MergeIteratorEntry(next, index));
                flag = false;
            }
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
        private Entry<MemorySegment> entry;

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
