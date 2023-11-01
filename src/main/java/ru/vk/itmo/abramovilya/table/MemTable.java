package ru.vk.itmo.abramovilya.table;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;

public class MemTable implements Table {
    private final Iterator<Entry<MemorySegment>> iterator;
    private MemTableEntry currentEntry;

    public MemTable(NavigableMap<MemorySegment, Entry<MemorySegment>> map) {
        iterator = map.values().iterator();
        if (iterator.hasNext()) {
            currentEntry = new MemTableEntry(iterator.next(), this);
        } else {
            currentEntry = new MemTableEntry(null, this);
        }
    }

    @Override
    public MemTableEntry nextEntry() {
        if (!iterator.hasNext()) {
            return null;
        }
        MemTableEntry nextEntry = new MemTableEntry(iterator.next(), this);
        currentEntry = nextEntry;
        return nextEntry;
    }

    @Override
    public TableEntry currentEntry() {
        return currentEntry;
    }
}
