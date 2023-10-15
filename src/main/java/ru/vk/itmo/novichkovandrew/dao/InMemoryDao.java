package ru.vk.itmo.novichkovandrew.dao;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.table.MemTable;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    protected final MemTable memTable;
    protected final Comparator<MemorySegment> comparator = (first, second) -> {
        if (first == null || second == null) return -1;
        long missIndex = first.mismatch(second);
        if (missIndex == first.byteSize()) {
            return -1;
        }
        if (missIndex == second.byteSize()) {
            return 1;
        }
        return missIndex == -1 ? 0 : Byte.compare(
                first.getAtIndex(ValueLayout.JAVA_BYTE, missIndex),
                second.getAtIndex(ValueLayout.JAVA_BYTE, missIndex)
        );
    };

    public InMemoryDao() {
        this.memTable = new MemTable(comparator);
    }

    @Override
    public Iterator<ru.vk.itmo.Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new Iterator<>() {
            final Iterator<MemorySegment> iterator = memTable.keyIterator(from, to);

            Entry<MemorySegment> entry = getEntry();

            private Entry<MemorySegment> getEntry() {
                Entry<MemorySegment> entry;
                do {
                    entry = iterator.hasNext() ? memTable.getEntry(iterator.next()) : null;
                } while (entry != null && iterator.hasNext() && memTable.isTombstone(entry));
                if (entry != null) {
                    return memTable.isTombstone(entry) ? null : entry;
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                return entry != null;
            }

            @Override
            public Entry<MemorySegment> next() {
                Entry<MemorySegment> entry = this.entry;
                this.entry = getEntry();
                return entry;
            }
        };
    }

    @Override
    public ru.vk.itmo.Entry<MemorySegment> get(MemorySegment key) {
        return memTable.getEntry(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.upsert(entry);
    }


    @Override
    public void close() throws IOException {
        memTable.close();
    }
}
