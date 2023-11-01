package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.Entry;

import java.util.Iterator;

public interface SSTable<D, E extends Entry<D>> {
    E get(D key);

    Iterator<E> scan(D keyFrom, D keyTo);
}
