package ru.vk.itmo.gorbokonenkolidiya;

import ru.vk.itmo.Entry;
import java.lang.foreign.MemorySegment;

public class InMemoryDao extends AbstractDao {
    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memTable.get(key);
    }
}
