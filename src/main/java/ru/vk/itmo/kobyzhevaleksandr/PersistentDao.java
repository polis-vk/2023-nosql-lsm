package ru.vk.itmo.kobyzhevaleksandr;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>>, Iterable<Entry<MemorySegment>> {

    private final MemorySegmentComparator memorySegmentComparator = new MemorySegmentComparator();
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> map =
        new ConcurrentSkipListMap<>(memorySegmentComparator);
    private final Storage storage;

    /*
    Filling ssTable with bytes from the memory segment with a structure:
    [key_size][key][value_size][value]...

    If value is null then value_size = -1
     */
    public PersistentDao(Config config) {
        this.storage = new Storage(config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> memoryIterator = getMemoryIterator(from, to);
        Iterator<Entry<MemorySegment>> storageIterator = storage.iterator(from, to);
        return new SkipNullIterator(GlobalIterator.merge(List.of(memoryIterator, storageIterator)));
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = map.get(key);
        if (entry != null && entry.value() != null) {
            return entry;
        }

        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }

        Entry<MemorySegment> next = iterator.next();
        if (memorySegmentComparator.compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Entry cannot be null.");
        }
        map.put(entry.key(), entry);
    }

    @Override
    public void compact() throws IOException {
        storage.compact(this);
    }

    @Override
    public void close() throws IOException {
        storage.save(map.values());
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return all();
    }

    private Iterator<Entry<MemorySegment>> getMemoryIterator(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return map.values().iterator();
        } else if (from == null) {
            return map.headMap(to).values().iterator();
        } else if (to == null) {
            return map.tailMap(from).values().iterator();
        }
        return map.subMap(from, to).values().iterator();
    }
}
