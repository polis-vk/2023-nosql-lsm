package ru.vk.itmo.proninvalentin;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.iterators.MergeIterator;
import ru.vk.itmo.proninvalentin.iterators.PeekingPriorityIterator;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.List;

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
        // Сначала ищем в памяти
        Entry<MemorySegment> ms = inMemoryDao.get(key);
        if (ms != null && ms.value() == null) {
            return null;
        }
        // Затем в файловой системе
        if (ms == null && fileDao != null) {
            Entry<MemorySegment> msFromFiles = fileDao.read(key);
            if (msFromFiles == null || msFromFiles.value() == null) {
                return null;
            }
            return msFromFiles;
        }

        return ms;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        List<PeekingPriorityIterator> inFileIterators = fileDao.getFileIterators(from, to);
        return new MergeIterator(inMemoryDao.get(from, to), inFileIterators);
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
