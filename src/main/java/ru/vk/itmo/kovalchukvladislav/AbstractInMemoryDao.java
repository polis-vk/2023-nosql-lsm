package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class AbstractInMemoryDao<D, E extends Entry<D>> implements Dao<D, E> {
    private final ConcurrentNavigableMap<D, E> dao;

    protected AbstractInMemoryDao(Comparator<? super D> comparator) {
        dao = new ConcurrentSkipListMap<>(comparator);
    }

    @Override
    public Iterator<E> get(D from, D to) {
        if (from == null && to == null) {
            return all();
        } else if (from == null) {
            return allTo(to);
        } else if (to == null) {
            return allFrom(from);
        }
        return dao.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public E get(D key) {
        return dao.get(key);
    }

    @Override
    public void upsert(E entry) {
        dao.put(entry.key(), entry);
    }

    @Override
    public Iterator<E> allFrom(D from) {
        if (from == null) {
            return all();
        }
        return dao.tailMap(from, false).values().iterator();
    }

    @Override
    public Iterator<E> allTo(D to) {
        if (to == null) {
            return all();
        }
        return dao.headMap(to, false).values().iterator();
    }

    @Override
    public Iterator<E> all() {
        return dao.values().iterator();
    }
}
