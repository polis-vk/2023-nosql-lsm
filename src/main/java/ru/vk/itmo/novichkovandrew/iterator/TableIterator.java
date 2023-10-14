package ru.vk.itmo.novichkovandrew.iterator;

import java.util.Iterator;

public interface TableIterator<T> extends Iterator<T> {
    int getTableNumber();
}
