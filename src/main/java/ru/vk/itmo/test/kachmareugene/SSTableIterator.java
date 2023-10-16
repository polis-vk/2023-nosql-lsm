package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.*; 

public class SSTableIterator implements Iterator<Entry<MemorySegment>> {

    private final Iterator<Entry<MemorySegment>> memTableIterator;
    private final SSTablesController controller;
    private final Comparator<MemorySegment> comp = new MemorySegmentComparator();
    private final SortedMap<MemorySegment, SSTableRowInfo> mp = new TreeMap<>(comp);
    private final MemorySegment from, to;
    private Entry<MemorySegment> head = null;


    public SSTableIterator(Iterator<Entry<MemorySegment>> it, SSTablesController controller, MemorySegment from, MemorySegment to) {
        memTableIterator = it;
        this.controller = controller;

        this.from = from;
        this.to = to;

        positioningIterator();

    }

    private void insertNew(SSTableRowInfo info) {
        Entry<MemorySegment> kv = controller.getRow(info);

        if (kv == null) {
            return;
        }

        if (!mp.containsKey(kv.key())) {
            mp.put(kv.key(), info);
            return;
        }
        SSTableRowInfo old = mp.get(kv.key());

        SSTableRowInfo oldInfo = old.SSTableInd > info.SSTableInd ? old : info;
        SSTableRowInfo newInfo = old.SSTableInd < info.SSTableInd ? info : old;

        mp.put(controller.getRow(newInfo).key(), newInfo);

        // tail recursion
        insertNew(controller.getNextInfo(oldInfo, to));
    }

    private void positioningIterator() {
        List<SSTableRowInfo> rawData = controller.firstGreaterKeys(from);

        for (var info : rawData) {
            insertNew(info);
        }
    }

    @Override
    public boolean hasNext() {
        return !mp.isEmpty() || hasHead();
    }

    private Entry<MemorySegment> getHead() {
        if (head == null) {
            head = memTableIterator.next();
        }
        return head;
    }
    private Entry<MemorySegment> moveAndGet() {
        getHead();
        var ans = head;
        head = null;
        return ans;
    }
    private boolean hasHead() {
        return head != null || memTableIterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        if (mp.isEmpty()) {
            return moveAndGet();
        }

        Map.Entry<MemorySegment, SSTableRowInfo> minSStablesEntry = mp.firstEntry();

        if (!hasHead()) {
            mp.remove(minSStablesEntry.getKey());
            insertNew(controller.getNextInfo(minSStablesEntry.getValue(), to));
            return controller.getRow(minSStablesEntry.getValue());
        }



        if (comp.compare(getHead().key(), minSStablesEntry.getKey()) < 0) {
            return moveAndGet();
        }

        mp.remove(minSStablesEntry.getKey());
        insertNew(controller.getNextInfo(minSStablesEntry.getValue(), to));

        if (comp.compare(getHead().key(), minSStablesEntry.getKey()) > 0) {
            return controller.getRow(minSStablesEntry.getValue());
        }

        return moveAndGet();
    }
}
