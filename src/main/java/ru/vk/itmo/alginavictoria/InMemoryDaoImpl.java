package ru.vk.itmo.alginavictoria;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class InMemoryDaoImpl extends AbstractDao {

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (to == null && from == null) {
            return dataMap.values().iterator();
        }
        if (from == null) {
            return dataMap.headMap(to).values().iterator();
        }
        if (to == null) {
            return dataMap.tailMap(from).values().iterator();
        }
        return dataMap.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return key == null ? null : dataMap.get(key);
    }

}
