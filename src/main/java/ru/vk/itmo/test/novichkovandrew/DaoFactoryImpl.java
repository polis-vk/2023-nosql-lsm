package ru.vk.itmo.test.novichkovandrew;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.Cell;
import ru.vk.itmo.novichkovandrew.MemorySegmentCell;
import ru.vk.itmo.novichkovandrew.dao.InMemoryDao;
import ru.vk.itmo.novichkovandrew.dao.PersistentDao;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 3)
public class DaoFactoryImpl implements DaoFactory.Factory<MemorySegment, Cell<MemorySegment>> {

    Cell.Factory<MemorySegment> factory = new MemorySegmentCell.CellFactory();

    @Override
    public Dao<MemorySegment, Cell<MemorySegment>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public Dao<MemorySegment, Cell<MemorySegment>> createDao(Config config) throws IOException {
        if (config == null || config.basePath() == null) {
            return createDao();
        }
        return new PersistentDao(config.basePath());
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        return new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Cell<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return factory.create(baseEntry);
    }
}
