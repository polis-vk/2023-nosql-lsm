package ru.vk.itmo.kovalchukvladislav.model;

import ru.vk.itmo.Entry;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;

public class InMemoryDaoStorage<D, E extends Entry<D>> implements DaoStorage<D, E> {
    private final Iterator<E> iterator;
    private InMemoryDaoEntry<D, E> currentEntry;
    private final Comparator<? super D> comparator;

    public InMemoryDaoStorage(NavigableMap<D, E> map, Comparator<? super D> comparator) {
        this.iterator = map.values().iterator();
        this.comparator = comparator;
        this.currentEntry = new InMemoryDaoEntry<>(iterator.next(), this, comparator);
    }

    @Override
    public DaoEntry<D, E> currentEntry() {
        return currentEntry;
    }

    @Override
    public DaoEntry<D, E> nextEntry() {
        if (!iterator.hasNext()) {
            return null;
        }
        InMemoryDaoEntry<D, E> nextEntry = new InMemoryDaoEntry<>(iterator.next(), this, comparator);
        currentEntry = nextEntry;
        return nextEntry;
    }
}
