package ru.vk.itmo.kovalchukvladislav;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

import ru.vk.itmo.Entry;

public class MemorySegmentDao extends AbstractInMemoryDao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<? super MemorySegment> COMPARATOR = getComparator();

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

            byte aByte = a.getAtIndex(ValueLayout.JAVA_BYTE, diffIndex);
            byte bByte = b.getAtIndex(ValueLayout.JAVA_BYTE, diffIndex);
            return Byte.compare(aByte, bByte);
        };
    }
}

