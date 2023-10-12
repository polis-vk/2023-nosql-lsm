package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface OutMemoryDao<D, E extends Entry<D>> extends AutoCloseable {

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
    void save(Map<D,E> map) throws IOException;

    /**
     * Returs iterator for every sstable, that was flushed in order from more new to more old.
     * @return list of sstable iterators.
     */
    List<Iterator<E>> get(D from, D to);

}
