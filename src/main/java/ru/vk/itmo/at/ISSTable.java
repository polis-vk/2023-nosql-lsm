package ru.vk.itmo.at;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import ru.vk.itmo.BaseEntry;

public interface ISSTable {
    BaseEntry<MemorySegment> get(MemorySegment key);

    void close() throws IOException;
}
