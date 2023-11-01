package ru.vk.itmo.novichkovandrew.table;

import ru.vk.itmo.novichkovandrew.Utils;
import ru.vk.itmo.novichkovandrew.exceptions.InvalidBlockException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class Footer {
    private Handle indexHandle;

    public Footer(Handle indexHandle) {
        this.indexHandle = indexHandle;
    }

    public void setIndexHandle(Handle indexHandle) {
        this.indexHandle = indexHandle;
    }

    public Handle getIndexHandle() {
        return indexHandle;
    }

    public static Footer createFooter(MemorySegment segment) {
        if (segment.byteSize() != Utils.FOOTER_SIZE) {
            throw new InvalidBlockException(
                    String.format("Invalid memory segment in footer place,"
                            + " expected: %s bytes, actual: %s", Utils.FOOTER_SIZE, segment.byteSize())
            );
        }
        long offset = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0L);
        long size = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES);
        return new Footer(new Handle(offset, size));
    }
}
