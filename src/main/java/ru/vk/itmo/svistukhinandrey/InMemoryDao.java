package ru.vk.itmo.svistukhinandrey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.svistukhinandrey.Utils;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    TreeSet<Entry<MemorySegment>> memorySegmentTreeMap;

    public InMemoryDao() {
        memorySegmentTreeMap = new TreeSet<>(Comparator.comparing(x -> {
            if (x == null) return null;
            if (x.key() == null) return null;
            return Utils.transform(x.key());
        }));
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();

        if (Utils.transform(next.key()).equals(Utils.transform(key))) {
            return next;
        }
        return null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memorySegmentTreeMap.iterator();
        }

        if (from == null) {
            return memorySegmentTreeMap.subSet(memorySegmentTreeMap.first(), findEntryByKey(memorySegmentTreeMap, to, KeyType.TO)).iterator();
        }

        if (to == null) {
            return memorySegmentTreeMap.subSet(findEntryByKey(memorySegmentTreeMap, from, KeyType.FROM), true, memorySegmentTreeMap.last(), false).iterator();
        }

        return memorySegmentTreeMap.subSet(findEntryByKey(memorySegmentTreeMap, from, KeyType.FROM), true, findEntryByKey(memorySegmentTreeMap, to, KeyType.TO), false).iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            return;
        }

        // If the entry doesn't exist, add it to the list
        memorySegmentTreeMap.add(entry);
    }

    enum KeyType {
        FROM,
        TO
    }

    public Entry<MemorySegment> findEntryByKey(TreeSet<Entry<MemorySegment>> treeSet, MemorySegment keyToFind, KeyType keyType) {
        Entry<MemorySegment> searchEntry = new BaseEntry<>(keyToFind, null); // Create a dummy entry with the key to search for
        Entry<MemorySegment> foundEntry = treeSet.ceiling(searchEntry);

        boolean same = false;
        if (foundEntry != null) {
            same = Utils.transform(foundEntry.key()).equals(Utils.transform(keyToFind));
//            same = foundEntry.key().getUtf8String(0).equals(keyToFind.getUtf8String(0));

        }

        if (foundEntry != null && same) {
            return foundEntry;
        } else {
            if (keyType == KeyType.FROM) {
                return treeSet.first();
            } else {
                return treeSet.last();
            }
        }
    }
}
