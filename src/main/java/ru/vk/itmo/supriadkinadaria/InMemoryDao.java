package ru.vk.itmo.supriadkinadaria;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(new InMemoryDaoComparator());
    private final SStableManipulations sStableManipulations;

    public InMemoryDao(Config config) throws IOException {
        this.sStableManipulations = new SStableManipulations(config);
//        read from file to storage
        if(Files.exists(sStableManipulations.getFilePath())) {
            sStableManipulations.readStorage(this);
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.values().iterator();
        } else if (to == null) {
            return storage.tailMap(from).values().iterator();
        } else if (from == null) {
            return storage.headMap(to).values().iterator();
        }
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("operation not supported");
    }

    @Override
    public void close() throws IOException {
//        write storage to file
        sStableManipulations.writeToFile(storage);
    }

    public static class InMemoryDaoComparator implements Comparator<MemorySegment> {
        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {
            long mismatch = o1.mismatch(o2);
            if (mismatch == -1) {
                return 0;
            } else if (o1.byteSize() == mismatch) {
                return -1;
            } else if (o2.byteSize() == mismatch) {
                return 1;
            }
            return o1.get(JAVA_BYTE, mismatch) - o2.get(JAVA_BYTE, mismatch);
        }
    }
}
