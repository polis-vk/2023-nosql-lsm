package ru.vk.itmo.trofimovmaxim;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
//    Map<MemorySegment, MemorySegment> data;
//    TreeSet<MemorySegment> orderedKeys;

//    TreeSet<Entry<MemorySegment>> data;

    final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data;

    Comparator<MemorySegment> compareSegment = (o1, o2) -> {
        if (o1 == null || o2 == null) {
            return o1 == null ? -1 : 1;
        }
        return Arrays.compare(
                o1.toArray(ValueLayout.OfByte.JAVA_BYTE),
                o2.toArray(ValueLayout.OfByte.JAVA_BYTE)
        );
    };

    public InMemoryDao() {
//        data = new HashMap<>();
//        orderedKeys = new TreeSet<>(compareSegment);

//        data = new TreeSet<>((o1, o2) -> compareSegment.compare(o1.key(), o2.key()));

//        data = new TreeMap<>(compareSegment);

        data = new ConcurrentSkipListMap<>(compareSegment);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new DaoIter(from, to);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
//        var it = get(key, null);
//        if (it.hasNext()) {
//            return it.next();
//        }
//        return null;

//        var res = data.floor(new BaseEntry<>(key, null));
//        if (compareSegment.compare(key, res.key()) == 0) {
//            return res;
//        }
//        return null;
//        System.out.println(data.get(key));
//
//        return new BaseEntry<>(key, data.get(key));

        return data.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
//        data.put(entry.key(), entry.value());
//        orderedKeys.add(entry.key());

//        data.add(entry);  // or copy?

        data.put(entry.key(), entry);
    }

    private class DaoIter implements Iterator<Entry<MemorySegment>> {
//        final Iterator<Entry<MemorySegment>> it = data.iterator();
//        Entry<MemorySegment> current = null;
//        MemorySegment to;

//        final Iterator<MemorySegment> it = orderedKeys.iterator();
//        MemorySegment current = null;
//        MemorySegment to;

        final Iterator<MemorySegment> it = data.navigableKeySet().iterator();
        MemorySegment current = null;
        MemorySegment to;

        DaoIter(MemorySegment from, MemorySegment to) {
//            this.to = to;
//            if (it.hasNext()) {
//                current = it.next();
//                while (compareSegment.compare(current.key(), from) < 0 && it.hasNext()) {
//                    current = it.next();
//                }
//                if (compareSegment.compare(current.key(), from) < 0) {
//                    if (it.hasNext()) {
//                        current = it.next();
//                    } else {
//                        current = null;
//                    }
//                }
//            }

            this.to = to;
            if (it.hasNext()) {
                current = it.next();
                while (compareSegment.compare(current, from) < 0 && it.hasNext()) {
                    current = it.next();
                }
                if (compareSegment.compare(current, from) < 0) {
                    if (it.hasNext()) {
                        current = it.next();
                    } else {
                        current = null;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public Entry<MemorySegment> next() {
//            var result = current;
//            Entry<MemorySegment> next = null;
//            if (it.hasNext()) {
//                next = it.next();
//                if (to != null && compareSegment.compare(next.key(), to) >= 0) {
//                    next = null;
//                }
//            }
//            current = next;
//            return result;

            var result = current;
            MemorySegment next = null;
            if (it.hasNext()) {
                next = it.next();
                if (to != null && compareSegment.compare(next, to) >= 0) {
                    next = null;
                }
            }
            current = next;
            return data.get(result);
        }
    }
}
