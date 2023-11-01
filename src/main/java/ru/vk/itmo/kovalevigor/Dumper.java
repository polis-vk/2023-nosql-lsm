package ru.vk.itmo.kovalevigor;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

public abstract class Dumper implements AutoCloseable {

    protected final MemorySegment memorySegment;
    protected long offset;

    protected Dumper(final Path path, final long fileSize, final Arena arena) throws IOException {
        memorySegment = UtilsMemorySegment.mapWriteSegment(path, fileSize, arena);
        offset = 0;
    }

    protected static long writeLong(final MemorySegment memorySegment, long offset, final long value) {
        memorySegment.set(ValueLayout.JAVA_LONG, offset, value);
        offset += ValueLayout.JAVA_LONG.byteSize();
        return offset;
    }

    protected void writeLong(final long value) {
        offset = writeLong(memorySegment, offset, value);
    }

    protected long writeMemorySegment(final MemorySegment segment, final long offset, final long size) {
        MemorySegment.copy(
                segment,
                0,
                memorySegment,
                offset,
                size
        );
        return offset + size;
    }

    protected void writeMemorySegment(final MemorySegment segment) {
        offset = writeMemorySegment(segment, offset, segment.byteSize());
    }

    protected abstract void writeHead();

    @Override
    public abstract void close() throws IOException;
}
