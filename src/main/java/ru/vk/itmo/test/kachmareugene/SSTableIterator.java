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
    private final Comparator<MemorySegment> comp = new MemSegComparatorNull();
    private final SortedMap<MemorySegment, SSTableRowInfo> mp = new TreeMap<>(comp);
    private final MemorySegment from;
    private final MemorySegment to;
    private Entry<MemorySegment> head;
    private Entry<MemorySegment> keeper;

    public SSTableIterator(Iterator<Entry<MemorySegment>> it, SSTablesController controller,
                           MemorySegment from, MemorySegment to) {
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

        SSTableRowInfo oldInfo = old.ssTableInd > info.ssTableInd ? info : old;
        SSTableRowInfo newInfo = old.ssTableInd < info.ssTableInd ? info : old;

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

    private void changeState() {
        while (true) {

            Map.Entry<MemorySegment, SSTableRowInfo> minSStablesEntry = getFirstMin();

            if (minSStablesEntry == null) {
                keeper = moveAndGet();
                return;
            }

            var curHead = getHead();

            if (curHead == null) {
                mp.remove(minSStablesEntry.getKey());
                insertNew(controller.getNextInfo(minSStablesEntry.getValue(), to));
                keeper = controller.getRow(minSStablesEntry.getValue());
                if (keeper != null && keeper.value() == null) {
                    continue;
                }
                return;
            }

            int res = comp.compare(curHead.key(), minSStablesEntry.getKey());

            if (res < 0) {
                keeper = moveAndGet();
                return;
            }

            mp.remove(minSStablesEntry.getKey());
            insertNew(controller.getNextInfo(minSStablesEntry.getValue(), to));

            if (res > 0) {
                keeper = controller.getRow(minSStablesEntry.getValue());
            } else {
                keeper = moveAndGetOnce();
            }

            if (keeper.value() != null) {
                break;
            }
        }
    }

    private boolean isBetween(MemorySegment who) {
        int res1 = comp.compare(from, who);
        int res2 = comp.compare(who, to);
        return res1 <= 0 && res2 < 0;
    }

    private Map.Entry<MemorySegment, SSTableRowInfo> getFirstMin() {
        Map.Entry<MemorySegment, SSTableRowInfo> minSStablesEntry = mp.firstEntry();

        while (!mp.isEmpty() && !isBetween(minSStablesEntry.getKey())) {
            mp.remove(minSStablesEntry.getKey());
            insertNew(controller.getNextInfo(minSStablesEntry.getValue(), to));

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
