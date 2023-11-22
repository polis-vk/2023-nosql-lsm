package ru.vk.itmo.viktorkorotkikh;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

public final class Utils {

    private Utils() {
    }

    public static long getEntrySize(Entry<MemorySegment> entry) {
        if (entry.value() == null) {
            return Long.BYTES + entry.key().byteSize() + Long.BYTES;
        }
        return Long.BYTES + entry.key().byteSize() + Long.BYTES + entry.value().byteSize();
    }
}
