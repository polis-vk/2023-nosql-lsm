package ru.vk.itmo.novichkovandrew.dao;

import ru.vk.itmo.Dao;
import ru.vk.itmo.novichkovandrew.Cell;
import ru.vk.itmo.novichkovandrew.MemorySegmentCell;
import ru.vk.itmo.novichkovandrew.table.MemTable;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;

public class InMemoryDao implements Dao<MemorySegment, Cell<MemorySegment>> {
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
    public Iterator<Cell<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new Iterator<>() {
            final Iterator<MemorySegment> iterator = memTable.keyIterator(from, to);

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Cell<MemorySegment> next() {
                Cell<MemorySegment> cell = null;
                while (iterator.hasNext() && (cell == null || cell.isTombstone())) {
                    cell = memTable.getCell(iterator.next());
                }
                return cell;
            }
        };
    }

    @Override
    public Cell<MemorySegment> get(MemorySegment key) {
        return memTable.getCell(key);
    }

    @Override
    public void upsert(Cell<MemorySegment> entry) {
        memTable.upsert(entry);
    }


    @Override
    public void close() throws IOException {
        memTable.close();
    }
}
