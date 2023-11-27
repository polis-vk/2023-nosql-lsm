package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class AbstractInMemoryDao<D, E extends Entry<D>> implements Dao<D, E> {
    protected final ConcurrentNavigableMap<D, E> dao;
    protected final Comparator<? super D> comparator;

    protected AbstractInMemoryDao(Comparator<? super D> comparator) {
        this.dao = new ConcurrentSkipListMap<>(comparator);
        this.comparator = comparator;
    }

    @Override
    public Iterator<E> get(D from, D to) {
        ConcurrentNavigableMap<D, E> subMap;
        if (from == null && to == null) {
            subMap = dao;
        } else if (from == null) {
            subMap = dao.headMap(to);
        } else if (to == null) {
            subMap = dao.tailMap(from);
        } else {
            subMap = dao.subMap(from, to);
        }
        return subMap.values().iterator();
    }

    @Override
    public E get(D key) {
        return dao.get(key);
    }

    @Override
    public void upsert(E entry) {
        dao.put(entry.key(), entry);
    }
}
