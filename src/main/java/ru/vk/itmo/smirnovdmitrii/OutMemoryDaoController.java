package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;

public interface OutMemoryDaoController extends Closeable {
    void compact();

    void flush(final Iterable<Entry<MemorySegment>> iterable);

    @Override
    void close();
}
