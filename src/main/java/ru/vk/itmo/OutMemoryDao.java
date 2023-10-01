package ru.vk.itmo;

import java.lang.foreign.MemorySegment;
import java.util.Map;

public interface OutMemoryDao<D, E extends Entry<D>> {

    /**
     * Returns value from disk storage. If value is not found then return null
     * @param key key for searching value.
     * @return founded value.
     */
    E get(D key);

    /**
     * Within this method you can save your in memory storage ({@link java.util.Map}) on disk. Truncates previous save.
     * @param map provided storage.
     */
    void save(Map<D,E> map);

}
