package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentDao extends AbstractInMemoryDao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<? super MemorySegment> COMPARATOR = getComparator();
    private static final ValueLayout.OfByte VALUE_LAYOUT = ValueLayout.JAVA_BYTE;

    public MemorySegmentDao() {
        super(COMPARATOR);
    }

    private static Comparator<? super MemorySegment> getComparator() {
        return (Comparator<MemorySegment>) (a, b) -> {
            long diffIndex = a.mismatch(b);
            if (diffIndex == -1) {
                return 0;
            } else if (diffIndex == a.byteSize()) {
                return -1;
            } else if (diffIndex == b.byteSize()) {
                return 1;
            }

            byte byteA = a.getAtIndex(VALUE_LAYOUT, diffIndex);
            byte byteB = b.getAtIndex(VALUE_LAYOUT, diffIndex);
            return Byte.compare(byteA, byteB);
        };
    }
}

