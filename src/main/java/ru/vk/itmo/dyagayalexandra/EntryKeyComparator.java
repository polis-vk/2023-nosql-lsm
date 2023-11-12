package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

public class EntryKeyComparator implements Comparator<Entry<MemorySegment>> {

    public static final Comparator<Entry<MemorySegment>> INSTANCE = new EntryKeyComparator();

    @Override
    public int compare(Entry<MemorySegment> entry1, Entry<MemorySegment> entry2) {
        return MemorySegmentComparator.INSTANCE.compare(entry1.key(), entry2.key());
    }
}
