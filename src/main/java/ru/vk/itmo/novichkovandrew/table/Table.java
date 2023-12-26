package ru.vk.itmo.novichkovandrew.table;

import ru.vk.itmo.novichkovandrew.iterator.TableIterator;

import java.io.Closeable;
import java.io.IOException;

public interface Table<K> extends Closeable {

    int size() throws IOException;

    TableIterator<K> tableIterator(K from, boolean fromInclusive, K to, boolean toInclusive);
}
