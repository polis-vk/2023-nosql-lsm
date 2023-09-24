package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    final Set<Entry<MemorySegment>> data;

    public InMemoryDao() {
        this.data = new TreeSet<>((o1, o2) -> compareMemorySegment(o1.key(), o2.key()));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return data.iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        data.add(entry);
    }

    private int compareMemorySegment(MemorySegment first, MemorySegment second) { //TODO #1: Fix allocate arrays;
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
