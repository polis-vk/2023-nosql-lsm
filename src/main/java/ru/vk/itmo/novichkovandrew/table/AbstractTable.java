package ru.vk.itmo.novichkovandrew.table;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public abstract class AbstractTable implements Table<MemorySegment> {
    protected final Comparator<MemorySegment> comparator = (first, second) -> {
        if (first == null || second == null) return -1; //TODO fix null. ?
        long missIndex = first.mismatch(second);
        if (missIndex == first.byteSize()) {
            return -1;
        }
        if (missIndex == second.byteSize()) {
            return 1;
        }
        return missIndex == -1 ? 0 : Byte.compare(
                first.getAtIndex(ValueLayout.JAVA_BYTE, missIndex),
                second.getAtIndex(ValueLayout.JAVA_BYTE, missIndex)
        );
    };

    public Comparator<MemorySegment> comparator() {
        return comparator;
    }

    public abstract void clear();
}
