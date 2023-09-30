package ru.vk.itmo;

import java.lang.foreign.MemorySegment;
import java.util.Map;

public interface OutMemoryDao<D, E extends Entry<D>> {
    Entry<MemorySegment> get(D key);

    void save(Map<D,E> map, long byteSize);

}
