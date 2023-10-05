package ru.vk.itmo.gamzatgadzhimagomedov;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

public class PersistentDao extends BaseDao{

    private final SSTable ssTable;

    public PersistentDao(Config config) {
        ssTable = new SSTable(config, new MemorySegmentComparator());
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (memTable.containsKey(key)) {
            return memTable.get(key);
        }
        return ssTable.get(key);
    }

    @Override
    public void close() throws IOException {
        if (!memTable.isEmpty()) {
            ssTable.flush(memTable.values());
        }
        memTable.clear();
    }
}
