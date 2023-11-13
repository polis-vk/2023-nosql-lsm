package ru.vk.itmo.kobyzhevaleksandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class DefaultPriorityPeekIterator extends DefaultPeekIterator
    implements PriorityPeekIterator<Entry<MemorySegment>> {

    private final int priority;

    public DefaultPriorityPeekIterator(Iterator<Entry<MemorySegment>> iterator, int priority) {
        super(iterator);
        this.priority = priority;
    }

    @Override
    public int priority() {
        return priority;
    }
}
