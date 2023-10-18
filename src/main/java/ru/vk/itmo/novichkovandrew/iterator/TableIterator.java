package ru.vk.itmo.novichkovandrew.iterator;

import ru.vk.itmo.Entry;

import java.util.Iterator;

public interface TableIterator<T> extends Iterator<Entry<T>> {
    int getTableNumber();
}
