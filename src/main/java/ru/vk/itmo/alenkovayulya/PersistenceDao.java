package ru.vk.itmo.alenkovayulya;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

public class PersistenceDao extends AbstractMemorySegmentDao {

    private final PersistentFileHandler persistentFileHandler;

    public PersistenceDao(Config config) {

        this.persistentFileHandler = new PersistentFileHandler(config);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {

        Entry<MemorySegment> memoryEntry = entries.get(key);

        if (memoryEntry == null) {
            return persistentFileHandler.readByKey(key);
        }

        return memoryEntry;
    }

    @Override
    public void close() throws IOException {
        if (!entries.isEmpty()) {
            persistentFileHandler.writeToFile(entries.values());
        }
        entries.clear();
    }
}
