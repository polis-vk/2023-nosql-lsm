package ru.vk.itmo.reshetnikovaleksei;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.ConcurrentNavigableMap;

public class PersistentDao extends BaseDao {
    private final SSTable ssTable;

    public PersistentDao(Config config) {
        this.ssTable = new SSTable(config, new MemorySegmentComparator());
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable = getMemoryTable();

        if (memoryTable.containsKey(key)) {
            return memoryTable.get(key);
        }

        try {
            return ssTable.get(key);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        if (!getMemoryTable().isEmpty()) {
            ssTable.save(getMemoryTable().values());
        }

        getMemoryTable().clear();
    }
}
