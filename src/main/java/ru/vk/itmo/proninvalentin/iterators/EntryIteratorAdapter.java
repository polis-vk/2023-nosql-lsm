package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.EnrichedEntry;
import ru.vk.itmo.proninvalentin.Metadata;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class EntryIteratorAdapter {
    public static Iterator<EnrichedEntry> create(Iterator<Entry<MemorySegment>> memorySegmentIterator) {
        return new Iterator<EnrichedEntry>() {
            @Override
            public boolean hasNext() {
                return memorySegmentIterator.hasNext();
            }

            @Override
            public EnrichedEntry next() {
                Entry<MemorySegment> curValue = memorySegmentIterator.next();
                return new EnrichedEntry(new Metadata(
                        0, curValue.value() == null, Long.MAX_VALUE),
                        curValue);
            }
        };
    }

}
