package ru.vk.itmo.abramovilya;

import java.lang.foreign.MemorySegment;

public interface Table extends Comparable<Table> {
    MemorySegment getValue();

    MemorySegment getKey();

    MemorySegment nextKey();

    void close();

    int number();

    @Override
    default int compareTo(Table other) {
        int compare = DaoImpl.compareMemorySegments(this.getKey(), other.getKey());
        if (compare != 0) {
            return compare;
        }
        return other.number() - this.number();
    }
}
