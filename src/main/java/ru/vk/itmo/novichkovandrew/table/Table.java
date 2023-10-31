package ru.vk.itmo.novichkovandrew.table;

import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.iterator.TableIterator;

import java.io.Closeable;
import java.lang.foreign.MemorySegment;

public interface Table<K> extends Iterable<Entry<MemorySegment>>, Closeable {

    int rows();

    TableIterator<K> tableIterator(K from, boolean fromInclusive, K to, boolean toInclusive);

    long byteSize();
}
