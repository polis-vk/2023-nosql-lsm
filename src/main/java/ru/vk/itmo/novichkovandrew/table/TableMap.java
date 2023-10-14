package ru.vk.itmo.novichkovandrew.table;

import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.iterator.PeekTableIterator;

import java.io.Closeable;
import java.io.IOException;

public interface TableMap<K, V> extends Closeable {

    Entry<K> getEntry(K key);

    K ceilKey(K key);

    default K floorKey(K key) { //TODO: Implement it
        throw new UnsupportedOperationException("Floor key operation is not supported yet");
    }

    int size() throws IOException;

    PeekTableIterator<K> iterator(K key, K to);
}
