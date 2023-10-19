package ru.vk.itmo.kobyzhevaleksandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class GlobalIterator {

    private static final MemorySegmentComparator memorySegmentComparator = new MemorySegmentComparator();

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
                MergePeekIterator mergePeekIterator = new MergePeekIterator(
                    new DefaultPeekIterator(iterators.get(0)),
                    new DefaultPeekIterator(iterators.get(1)),
                    memorySegmentComparator
                );

                for (int i = 2; i < iterators.size(); i++) {
                    mergePeekIterator = new MergePeekIterator(
                        mergePeekIterator,
                        new DefaultPeekIterator(iterators.get(i)),
                        memorySegmentComparator
                    );
                }

                return mergePeekIterator;
            }
        }
    }
}
