package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class SSTableIterator implements Iterator<Entry<MemorySegment>> {

    private final Iterator<Entry<MemorySegment>> memTableIterator;
    private final SSTablesController controller;
    private final Comparator<MemorySegment> comp;
    private final SortedMap<MemorySegment, SSTableRowInfo> mp;
    private final MemorySegment from;
    private final MemorySegment to;
    private Entry<MemorySegment> head;
    private Entry<MemorySegment> keeper;
    private boolean isReversed;

    public SSTableIterator(Iterator<Entry<MemorySegment>> it, SSTablesController controller,
                           MemorySegment from, MemorySegment to) {
        this.memTableIterator = it;
        this.controller = controller;
        this.comp = new MemSegComparatorNull();
        this.mp = new TreeMap<>(comp);

        this.from = from;
        this.to = to;

        positioningIterator();
    }

    public SSTableIterator(Iterator<Entry<MemorySegment>> it, SSTablesController controller,
                           MemorySegment from, MemorySegment to, boolean isReversed, Comparator<MemorySegment> comp) {
        this.memTableIterator = it;
        this.controller = controller;
        this.comp = comp;
        this.mp = new TreeMap<>(comp);

        this.from = from;
        this.to = to;
        this.isReversed = isReversed;

        positioningIterator();
    }

    private void positioningIterator() {
        List<SSTableRowInfo> rawData = controller.firstKeys(from, isReversed);

        for (var info : rawData) {
            IteratorUtils.insertNew(mp, controller, info, to);
        }
    }

    @Override
    public boolean hasNext() {
        if (keeper == null) {
            changeState();
        }
        return keeper != null;
    }

    private Entry<MemorySegment> getHead() {
        if (head == null && memTableIterator.hasNext()) {
            head = memTableIterator.next();
            if (comp.compare(head.key(), to) >= 0) {
                head = null;
            }
        }
        return head;
    }

    private Entry<MemorySegment> moveAndGet() {
        Entry<MemorySegment> ans;
        do {
            getHead();
            ans = head;
            head = null;
        } while (ans != null && ans.value() == null);
        return ans;
    }

    private Entry<MemorySegment> moveAndGetOnce() {
        getHead();
        var ans = head;
        head = null;
        return ans;
    }

    private boolean isChangedByNulls(Map.Entry<MemorySegment, SSTableRowInfo> minSStablesEntry) {
        if (minSStablesEntry == null) {
            keeper = moveAndGet();
            return true;
        }
        if (getHead() == null) {
            updateMp(minSStablesEntry.getKey());
            keeper = controller.getRow(minSStablesEntry.getValue());
            return keeper == null || keeper.value() != null;
        }
        return false;
    }

    private boolean isChangedByStructures(Map.Entry<MemorySegment, SSTableRowInfo> minSStablesEntry) {
        var currHead = getHead();
        if (currHead == null) {
            return false;
        }
        int res = comp.compare(currHead.key(), minSStablesEntry.getKey());

        if (res < 0) {
            keeper = moveAndGet();
            return true;
        }

        updateMp(minSStablesEntry.getKey());
        keeper = (res > 0) ? controller.getRow(minSStablesEntry.getValue()) : moveAndGetOnce();
        return keeper.value() != null;
    }

    private void changeState() {
        while (true) {
            Map.Entry<MemorySegment, SSTableRowInfo> minSStablesEntry = getFirstMin();

            if (isChangedByNulls(minSStablesEntry) || isChangedByStructures(minSStablesEntry)) {
                return;
            }
        }
    }

    private void updateMp(MemorySegment key) {
        if (isReversed) {
            IteratorUtils.insertNew(mp, controller,
                    controller.getPrevInfo(mp.remove(key), to), to);
            return;
        }
        IteratorUtils.insertNew(mp, controller,
                controller.getNextInfo(mp.remove(key), to), to);
    }

    private boolean isBetween(MemorySegment who) {
        int res1 = comp.compare(from, who);
        int res2 = comp.compare(who, to);
        return res1 <= 0 && res2 < 0;
    }

    private Map.Entry<MemorySegment, SSTableRowInfo> getFirstMin() {
        Map.Entry<MemorySegment, SSTableRowInfo> minSStablesEntry = mp.firstEntry();
        while (!mp.isEmpty() && !isBetween(minSStablesEntry.getKey())) {
            updateMp(minSStablesEntry.getKey());
            minSStablesEntry = mp.firstEntry();
        }
        return minSStablesEntry;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (keeper == null) {
            changeState();
        }
        var ans = keeper;
        keeper = null;
        return ans;
    }
}
