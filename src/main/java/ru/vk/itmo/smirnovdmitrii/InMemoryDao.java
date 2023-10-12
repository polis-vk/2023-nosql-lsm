package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.util.SortedMap;

public interface InMemoryDao<D, E extends Entry<D>> extends Dao<D, E> {

    SortedMap<D, E> getMap();
}
