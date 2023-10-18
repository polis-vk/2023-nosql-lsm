package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class MemEntryShiftedIterator extends ShiftedIterator<Entry<MemorySegment>>
        implements Comparable<MemEntryShiftedIterator> {

    public MemEntryShiftedIterator(Iterator<Entry<MemorySegment>> iterator) {
        super(iterator);
    }

    @Override
    public int compareTo(final MemEntryShiftedIterator rhs) {
        return UtilsMemorySegment.compareEntry(value, rhs.value);
    }
}
