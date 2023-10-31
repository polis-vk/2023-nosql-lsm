package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class Utils {
    private Utils() {
    }

    public static Entry<MemorySegment> getEntryByKeyOffset(
            long offsetResult, MemorySegment offsetSegment, MemorySegment dataSegment) {

        long offset = offsetResult + Long.BYTES;
        long valueOffset = offsetSegment.get(ValueLayout.JAVA_LONG, offset);

        MemorySegment valueSegment;
        offset += Long.BYTES;
        if (offset >= offsetSegment.byteSize()) {
            valueSegment = dataSegment.asSlice(valueOffset);

        } else {
            long valueSize = offsetSegment.get(ValueLayout.JAVA_LONG, offset) - valueOffset;

            valueSegment = dataSegment.asSlice(valueOffset, valueSize);
        }

        if (valueSegment.byteSize() == Long.BYTES && valueSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0) == -1) {
            valueSegment = null;
        }

        long keyOffset = offsetSegment.get(ValueLayout.JAVA_LONG, offsetResult);
        long keySize = valueOffset - keyOffset;
        MemorySegment keySegment = dataSegment.asSlice(keyOffset, keySize);

        return new BaseEntry<>(keySegment, valueSegment);
    }
}
