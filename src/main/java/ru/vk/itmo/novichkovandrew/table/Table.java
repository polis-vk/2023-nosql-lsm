package ru.vk.itmo.novichkovandrew.table;

import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.iterator.TableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;

public interface Table<K> extends Closeable {

    int size() throws IOException;

    TableIterator<K> tableIterator(K from, boolean fromInclusive, K to, boolean toInclusive);
}
