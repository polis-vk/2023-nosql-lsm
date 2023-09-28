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
            return allToUnsafe(to);
        } else if (to == null) {
            return allFromUnsafe(from);
        }
        return dao.subMap(from, to).values().iterator();
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
        return from == null ? all() : allFromUnsafe(from);
    }

    /**
     * Doesn't check the argument for null. Should be called only if there was a check before
     * @param from NotNull lower bound of range (inclusive)
     * @return entries with key >= from
     */
    private Iterator<E> allFromUnsafe(D from) {
        return dao.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<E> allTo(D to) {
        return to == null ? all() : allToUnsafe(to);
    }

    /**
     * Doesn't check the argument for null. Should be called only if there was a check before
     * @param to NotNull upper bound of range (exclusive)
     * @return entries with key < to
     */
    private Iterator<E> allToUnsafe(D to) {
        return dao.headMap(to).values().iterator();
    }

    @Override
    public Iterator<E> all() {
        return dao.values().iterator();
    }
}
