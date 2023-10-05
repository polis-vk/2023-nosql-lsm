package ru.vk.itmo.gamzatgadzhimagomedov;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

public class InMemoryDao extends BaseDao {

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memTable.get(key);
    }
}
