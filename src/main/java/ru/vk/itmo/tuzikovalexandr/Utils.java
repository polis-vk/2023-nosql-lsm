package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static java.lang.Integer.parseInt;

public final class Utils {
    private Utils() {
    }

    public static Entry<MemorySegment> getEntryByKeyOffset(
            long offsetResult, MemorySegment offsetSegment, MemorySegment dataSegment) {

        long keyOffset = offsetSegment.get(ValueLayout.JAVA_LONG, offsetResult);

        long offset = offsetResult + Long.BYTES;
        long keySize = offsetSegment.get(ValueLayout.JAVA_LONG, offset) - keyOffset;
        MemorySegment keySegment = dataSegment.asSlice(keyOffset, keySize);

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

        return new BaseEntry<>(keySegment, valueSegment);
    }

    public static Integer getIntIndexFromString(String fileName) {
        String pattern = "data_";

        int startIdx = fileName.indexOf(pattern) + pattern.length();
        return parseInt(fileName.substring(startIdx), 10);
    }
}
