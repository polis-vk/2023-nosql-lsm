package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.util.SortedMap;

public interface InMemoryDao<D, E extends Entry<D>> extends Dao<D, E> {

    /**
     * returns {@link java.util.SortedMap} representing elements in memory.
     * Changing this map can produce bad work of {@link InMemoryDao}.
     * @return sorted map of elements in memory.
     */

    SortedMap<D, E> getMap();
}
