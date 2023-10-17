package ru.vk.itmo.abramovilya.table;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

public class MemTableEntry implements TableEntry {
    private final Entry<MemorySegment> current;
    private final MemTable memTable;

    public MemTableEntry(Entry<MemorySegment> entry, MemTable memTable) {
        current = entry;
        this.memTable = memTable;
    }

    @Override
    public MemorySegment getValue() {
        return current.value();
    }

    @Override
    public MemorySegment getKey() {
        return current.key();
    }

    @Override
    public int number() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Table table() {
        return memTable;
    }
}
