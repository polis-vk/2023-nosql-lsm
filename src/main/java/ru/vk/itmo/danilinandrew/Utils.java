package ru.vk.itmo.danilinandrew;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;

public class Utils {
    public static Iterator<Entry<MemorySegment>> getIterFrom(
            NavigableMap<MemorySegment, Entry<MemorySegment>> map,
            MemorySegment from,
            MemorySegment to
    ) {
        if (from == null && to == null) {
            return map.values().iterator();
        }
        if (from == null) {
            return map.headMap(to).values().iterator();
        }
        if (to == null) {
            return map.tailMap(from).values().iterator();
        }
        return map.subMap(from, to).values().iterator();
    }

    public static long getByteSize(Entry<MemorySegment> entry) {
        long valueSize = entry.value() == null ? 0 : entry.value().byteSize();
        return (entry.key().byteSize() + valueSize) * 2L;
    }
}
