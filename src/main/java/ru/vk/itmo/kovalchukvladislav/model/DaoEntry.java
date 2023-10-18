package ru.vk.itmo.kovalchukvladislav.model;

import ru.vk.itmo.Entry;

public interface DaoEntry<D, E extends Entry<D>> extends Comparable<DaoEntry<D, E>> {
    E getEntry();

    DaoStorage<D, E> storage();
}
