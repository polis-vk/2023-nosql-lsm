package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    final Set<Entry<MemorySegment>> data;

    public InMemoryDao() {
        this.data = new TreeSet<>((o1, o2) -> compareMemorySegment(o1.key(), o2.key()));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        final Iterator<Entry<MemorySegment>> iterator = data.iterator();
        Entry<MemorySegment> value = null;
        while (iterator.hasNext()) {
            value = iterator.next();
            if (compareMemorySegment(from, value.key()) <= 0) {
                break;
            }
        }
        Entry<MemorySegment> finalValue = value;
        return new Iterator<>() {
            Entry<MemorySegment> val = finalValue;

            @Override
            public boolean hasNext() {
                if (val == null) return false;
                return compareMemorySegment(val.key(), to) < 0;
            }

            @Override
            public Entry<MemorySegment> next() {
                var result = val;
                val = iterator.hasNext() ? iterator.next() : null;
                return result;
            }
        };
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> nextKey = iterator.next();
        if (equals(nextKey.key(), key)) {
            return nextKey;
        }
        return null;
    }


    private boolean equals(MemorySegment a, MemorySegment b) {
        if (a == b) return true;
        if (a == null || a.getClass() != b.getClass()) return false;
        return compareMemorySegment(a, b) == 0;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        data.add(entry);
    }

    private int compareMemorySegment(MemorySegment first, MemorySegment second) { //TODO #1: Fix allocate arrays;
        if (first == null || second == null) return -1;
        byte[] f = first.toArray(ValueLayout.JAVA_BYTE);
        byte[] s = second.toArray(ValueLayout.JAVA_BYTE);
        if (f.length != s.length) return Integer.compare(f.length, s.length);
        for (int i = 0; i < f.length; i++) {
            if (f[i] != s[i]) {
                return Integer.compare(f[i], s[i]);
            }
        }
        return 0;
    }
}
