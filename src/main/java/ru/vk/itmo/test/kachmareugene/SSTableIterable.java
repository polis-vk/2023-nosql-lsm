package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.Iterator;

public class SSTableIterable implements Iterable<Entry<MemorySegment>> {
    private final Collection<Entry<MemorySegment>> memTable;
    private final SSTablesController controller;
    private final MemorySegment from;
    private final MemorySegment to;

    public SSTableIterable(Collection<Entry<MemorySegment>> it, SSTablesController controller,
                           MemorySegment from, MemorySegment to) {
        this.memTable = it;
        this.controller = controller;

        this.from = from;
        this.to = to;
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return new SSTableIterator(memTable.iterator(), controller, from, to);
    }
}
