package ru.vk.itmo.alenkovayulya;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

public class InMemoryDao extends AbstractMemorySegmentDao {

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return entries.get(key);
    }

}
