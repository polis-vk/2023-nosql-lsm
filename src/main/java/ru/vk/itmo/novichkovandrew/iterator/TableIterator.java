package ru.vk.itmo.novichkovandrew.iterator;

import ru.vk.itmo.Entry;

import java.util.Iterator;
import java.util.function.Consumer;

public interface TableIterator<T> extends Iterator<Entry<T>> {
    int getTableNumber();
}
