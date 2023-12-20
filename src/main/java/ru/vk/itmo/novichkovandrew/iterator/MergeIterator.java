package ru.vk.itmo.novichkovandrew.iterator;

import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.Candidate;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {
    private final Queue<Candidate<MemorySegment>> queue;
    private Entry<MemorySegment> minEntry;
    private MemorySegment lowerBoundKey;
    private final Comparator<MemorySegment> memoryComparator;

    public MergeIterator(List<TableIterator<MemorySegment>> iterators, Comparator<MemorySegment> memoryComparator) {
        this.memoryComparator = memoryComparator;
        this.queue = iterators.stream()
                .map(it -> new Candidate<>(it, memoryComparator))
                .filter(Candidate::nonLast)
                .collect(Collectors.toCollection(PriorityBlockingQueue::new));
        minEntry = getMinEntry();
    }

    private Entry<MemorySegment> getMinEntry() {
        if (queue.isEmpty()) {
            return null;
        }
        while (!queue.isEmpty()) {
            var candidate = queue.poll();
            if (lowerBoundKey == null || memoryComparator.compare(lowerBoundKey, candidate.entry().key()) < 0) {
                lowerBoundKey = candidate.entry().key();
                var entry = candidate.entry();
                candidate.update();
                if (candidate.nonLast()) {
                    queue.add(candidate);
                }
                if (entry.value() != null) { // Tombstone check.
                    return entry;
                }
            } else {
                candidate.update();
                if (candidate.nonLast()) {
                    queue.add(candidate);
                }
            }
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        return this.minEntry != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        var entry = this.minEntry;
        this.minEntry = getMinEntry();
        return entry;
    }
}
