package ru.vk.itmo.at;

import java.lang.foreign.MemorySegment;

import ru.vk.itmo.BaseEntry;

public class SSTableEmpty implements ISSTable {
    public static final SSTableEmpty INSTANCE = new SSTableEmpty();

    private SSTableEmpty() {
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) {
        return null;
    }

    @Override
    public void close() {
    }
}
