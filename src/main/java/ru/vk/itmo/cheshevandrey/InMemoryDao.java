package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.cheshevandrey.InMemoryFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> map = new ConcurrentSkipListMap<>(
            (fSeg, sSeg) -> {
                int fSegSize = (int) fSeg.byteSize();
                int sSegSize = (int) sSeg.byteSize();

                if (fSegSize == sSegSize) {
                    int offset = 0;
                    for (int i = fSegSize; i > 0; i--) {
                        byte firstByte = fSeg.get(ValueLayout.JAVA_BYTE, offset);
                        byte secondByte = sSeg.get(ValueLayout.JAVA_BYTE, offset);
                        if (firstByte > secondByte) {
                            return 1;
                        } else if (firstByte < secondByte) {
                            return -1;
                        }
                        offset++;
                    }
                    return 0;
                } else if (fSegSize > sSegSize) {
                    return 1;
                } else {
                    return -1;
                }
            }
    );

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return map.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (map.size() == 0) {
            return map.values().iterator();
        }

        MemorySegment first = (from == MemorySegment.NULL) ? map.firstKey() : map.ceilingKey(from);
        MemorySegment last = null;

        if (to != MemorySegment.NULL) {
            last = map.ceilingKey(to);
        }

        return (last == null) ?
                map.tailMap(first, true).values().iterator() :
                map.subMap(first, last).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        map.put(entry.key(), entry);
    }
}