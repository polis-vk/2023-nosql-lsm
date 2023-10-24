package ru.vk.itmo.peskovalexandr;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import java.io.IOException;
import java.lang.foreign.MemorySegment;

public class PersistentDao extends InMemoryDao {

    private final SSTable ssTable;

    public PersistentDao(Config config) {
        ssTable = new SSTable(config);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (entryMap.containsKey(key)) {
            return entryMap.get(key);
        }
        return ssTable.get(key);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        ssTable.saveEntryMap(entryMap);
    }
}
