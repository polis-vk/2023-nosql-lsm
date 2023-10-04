package ru.vk.itmo.kovalevigor;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class MemoryEntryFileWorker {

    private long offset;
    public final MemorySegment data;
    public static final ValueLayout.OfLong META_LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;
    public static final long ENTRY_META_SIZE = META_LAYOUT.byteSize() * 2;

    protected MemoryEntryFileWorker(final MemorySegment data, final long offset) {
        this.data = data;
        this.offset = offset;
    }

    protected MemoryEntryFileWorker(final MemorySegment data) {
        this(data, 0);
    }

    protected long getOffset() {
        return offset;
    }

    protected boolean enoughCapacity(final long offset, final long size) {
        return offset <= data.byteSize() - size;
    }

    protected boolean enoughCapacity(final long size) {
        return enoughCapacity(getOffset(), size);
    }

    protected boolean enoughForMeta() {
        return enoughCapacity(META_LAYOUT.byteSize());
    }

    protected long changeOffset(final long diff) {
        offset += diff;
        return getOffset();
    }

}
