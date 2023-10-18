package ru.vk.itmo.kovalchukvladislav.model;

import ru.vk.itmo.Entry;

public interface DaoStorage<D, E extends Entry<D>> {
    DaoEntry<D, E> currentEntry();

    DaoEntry<D, E> nextEntry();
}
