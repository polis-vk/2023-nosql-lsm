package ru.vk.itmo.gorbokonenkolidiya;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

public class PersistentDao extends AbstractDao {

    private final SSTable ssTableHandler;

    public PersistentDao(Config config) {
        ssTableHandler = new SSTable(config);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (memTable.containsKey(key)) {
            return memTable.get(key);
        }
        return ssTableHandler.get(key);
    }

    @Override
    public void close() throws IOException {
        if (!memTable.isEmpty()) {
            ssTableHandler.flush(memTable.values());
        }
        memTable.clear();
    }
}
