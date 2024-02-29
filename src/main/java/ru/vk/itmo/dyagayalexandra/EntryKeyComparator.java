package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

public class EntryKeyComparator implements Comparator<Entry<MemorySegment>> {

    private final MemorySegmentComparator memorySegmentComparator;

    public EntryKeyComparator(MemorySegmentComparator memorySegmentComparator) {
        this.memorySegmentComparator = memorySegmentComparator;
    }

    @Override
    public int compare(Entry<MemorySegment> entry1, Entry<MemorySegment> entry2) {
        return memorySegmentComparator.compare(entry1.key(), entry2.key());
    }
}
