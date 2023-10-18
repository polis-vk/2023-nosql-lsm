package ru.vk.itmo.bazhenovkirill;

import java.util.Comparator;

public class SSTableIteratorComparator implements Comparator<SSTablePeekableIterator> {

    private final MemorySegmentComparator cmp = MemorySegmentComparator.getInstance();

    @Override
    public int compare(SSTablePeekableIterator o1, SSTablePeekableIterator o2) {
        int comparisonResult = cmp.compare(o1.getCurrentEntry().key(), o2.getCurrentEntry().key());
        if (comparisonResult == 0) {
            if (o1.getCurrentTimestamp() == o2.getCurrentTimestamp()) return 0;
            return o1.getCurrentTimestamp() < o2.getCurrentTimestamp() ? 1 : -1;
        }
        return comparisonResult;
    }
}
