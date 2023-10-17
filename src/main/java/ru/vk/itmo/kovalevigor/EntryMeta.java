package ru.vk.itmo.kovalevigor;

import java.lang.foreign.MemorySegment;

public class EntryMeta {

    public final long keyOffset;
    public final long keySize;
    public final long valueOffset;
    public final long valueSize;

    public EntryMeta(final long keyOffset, final long keySize, final long valueOffset, final long valueSize) {
        this.keyOffset = keyOffset;
        this.keySize = keySize;
        this.valueOffset = valueOffset;
        this.valueSize = valueSize;
    }
}
