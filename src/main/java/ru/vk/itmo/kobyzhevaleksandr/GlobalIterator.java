package ru.vk.itmo.kobyzhevaleksandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class GlobalIterator {

    private static final PriorityPeekIteratorComparator PRIORITY_PEEK_ITERATOR_COMPARATOR =
        new PriorityPeekIteratorComparator(new MemorySegmentComparator());

    private GlobalIterator() {
    }

    public static PeekIterator<Entry<MemorySegment>> merge(List<Iterator<Entry<MemorySegment>>> iterators) {
        switch (iterators.size()) {
            case 0 -> {
                return new DefaultPeekIterator(Collections.emptyIterator());
            }
            case 1 -> {
                return new DefaultPeekIterator(iterators.getFirst());
            }
            default -> {
                List<PriorityPeekIterator<Entry<MemorySegment>>> priorityIterators = new ArrayList<>(iterators.size());
                for (int i = 0; i < iterators.size(); i++) {
                    priorityIterators.add(new DefaultPriorityPeekIterator(iterators.get(i), i));
                }

                return new MergePriorityPeekIterator(priorityIterators, PRIORITY_PEEK_ITERATOR_COMPARATOR);
            }
        }
    }
}
