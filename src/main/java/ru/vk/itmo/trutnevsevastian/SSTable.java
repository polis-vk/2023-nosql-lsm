package ru.vk.itmo.trutnevsevastian;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class SSTable {

    private final MemorySegment data;

    private final Comparator<MemorySegment> comparator;

    public SSTable(MemorySegment data, Comparator<MemorySegment> comparator) {
        this.data = data;
        this.comparator = comparator;
    }

    public MemorySegment get(MemorySegment key) {
        long offset = 0;
        while (offset < data.byteSize()) {
            long keySize = data.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            MemorySegment entryKey = data.asSlice(offset, keySize);
            offset += keySize;
            long valueSize = data.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            if (comparator.compare(entryKey, key) == 0) {
                return data.asSlice(offset, valueSize);
            }
            offset += valueSize;
        }
        return null;
    }
}
