package ru.vk.itmo.smirnovdmitrii.transaction;

import ru.vk.itmo.Entry;

public interface Transaction<T, E extends Entry<T>> {


    E get(T t);

    void upsert(E e);

    void commit();
}
