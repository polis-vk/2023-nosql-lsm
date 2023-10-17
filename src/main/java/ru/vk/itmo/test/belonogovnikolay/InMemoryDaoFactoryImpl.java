package ru.vk.itmo.test.belonogovnikolay;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.belonogovnikolay.InMemoryTreeDao;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@DaoFactory(stage = 2, week = 2)
public class InMemoryDaoFactoryImpl implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    /**
     * Creates new instance of Dao.
     *
     * @return Dao.
     */
    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return InMemoryTreeDao.newInstance();
    }

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) {
        return InMemoryTreeDao.newInstance(config);
    }


    /**
     * Converts data represented in MemorySegment format to String format.
     *
     * @param memorySegment data in MemorySegment representation.
     * @return representation of MemorySegment input data as a String.
     */
    @Override
    public String toString(MemorySegment memorySegment) {
        if (Objects.isNull(memorySegment)) {
            return null;
        }
        return new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    /**
     * Converts data represented in String format to MemorySegment format.
     *
     * @param data data in String representation.
     * @return representation of input data as a MemorySegment
     */
    @Override
    public MemorySegment fromString(String data) {
        if (Objects.isNull(data)) {
            return null;
        }

        return MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
