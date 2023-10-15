package ru.vk.itmo.abramovilya;

import java.lang.foreign.MemorySegment;

public interface Table {
    MemorySegment getValueFromStorage();

    MemorySegment nextKey();

    void close();

    int number();
}
