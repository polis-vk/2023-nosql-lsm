package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Collection;

public class PersistentStorage {
    // TODO заменить на интерфейс сстейбл
    private final SimpleSSTable sstable;

    PersistentStorage(Path basePath) {
        this.sstable = new SimpleSSTable(basePath);
    }

    public void store(Collection<Entry<MemorySegment>> data) {
        long dataSize = 0;
        for (var entry: data) {
            dataSize += entry.value().byteSize() + entry.key().byteSize() + 16;
        }
        sstable.writeEntries(data.iterator(), dataSize);
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        var ssTableResult = this.sstable.get(key);
        return ssTableResult == null ? null : new Entry<>() {
            @Override
            public MemorySegment key() {
                return key;
            }

            @Override
            public MemorySegment value() {
                return ssTableResult;
            }
        };
    }
}
