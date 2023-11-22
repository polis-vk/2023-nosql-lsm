package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Entry;

import java.util.Iterator;

public interface InMemoryDao<D, E extends Entry<D>> extends AutoCloseable {

    /**
     * Committing state of elements in memory represented as map.
     * Returns {@link Iterable} representing sorted elements in memory.
     * After that in memory dao will be empty.
     * Changing this map can produce bad work of {@link InMemoryDao}.
     * @return sorted map of elements in memory.
     */
    Iterable<E> commit();

    /**
     * Return iterator for data in memory from key {@code from} to key {@code to}.
     * @param from from key.
     * @param to to key.
     * @return returned iterator.
     */
    Iterator<Entry<D>> get(D from, D to);

    /**
     * Return entry that associated with key {@code key}. Null if there is no entry with such key.
     * @param key key to search.
     * @return entry associated with key.
     */
    Entry<D> get(D key);

    /**
     * Adding entry to in memory dao. If there was entry with same key, then replace it.
     * @param entry entry to add.
     */
    void upsert(Entry<D> entry);

    @Override
    void close();
}
