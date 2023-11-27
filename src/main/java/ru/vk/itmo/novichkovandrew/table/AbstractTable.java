package ru.vk.itmo.novichkovandrew.table;

import ru.vk.itmo.novichkovandrew.Utils;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Objects;

public abstract class AbstractTable implements Table<MemorySegment> {
    protected final Comparator<MemorySegment> comparator = (first, second) -> {
        Objects.requireNonNull(first, "First segment is null in memory comparing");
        Objects.requireNonNull(second, "Second segment is null in memory comparing");
        if (first == Utils.LEFT || second == Utils.RIGHT) {
            return -1;
        }
        if (first == Utils.RIGHT || second == Utils.LEFT) {
            return 1;
        }
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
}
