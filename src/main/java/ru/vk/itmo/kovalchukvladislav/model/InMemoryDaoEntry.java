package ru.vk.itmo.kovalchukvladislav.model;

import ru.vk.itmo.Entry;
import java.util.Comparator;

public class InMemoryDaoEntry<D, E extends Entry<D>> implements DaoEntry<D, E> {
    private final E currentEntry;
    private final InMemoryDaoStorage<D, E> storage;
    private final Comparator<? super D> comparator;

    public InMemoryDaoEntry(E entry, InMemoryDaoStorage<D, E> storage, Comparator<? super D> comparator) {
        this.currentEntry = entry;
        this.storage = storage;
        this.comparator = comparator;
    }

    @Override
    public E getEntry() {
        return currentEntry;
    }

    @Override
    public DaoStorage<D, E> storage() {
        return storage;
    }

    @Override
    public int compareTo(DaoEntry<D, E> other) {
        return comparator.compare(currentEntry.key(), other.getEntry().key());
    }
}
