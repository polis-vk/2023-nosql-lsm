package ru.vk.itmo.proninvalentin;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class DaoFacade implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final FileDao fileDao;
    private final InMemoryDao inMemoryDao;

    public DaoFacade() {
        fileDao = null;
        inMemoryDao = new InMemoryDao();
    }

    public DaoFacade(Config config) throws IOException {
        fileDao = new FileDao(config);
        inMemoryDao = new InMemoryDao();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> ms = inMemoryDao.get(key);
        if (ms == null && fileDao != null) {
            ms = fileDao.read(key);
        }
        return ms;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return inMemoryDao.get(from, to);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        inMemoryDao.upsert(entry);
    }

    @Override
    public void close() throws IOException {
        if (fileDao != null) {
            fileDao.write(inMemoryDao);
            fileDao.close();
        }
    }
}
