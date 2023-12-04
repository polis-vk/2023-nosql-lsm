package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class DaoWithCompaction extends InMemoryDao {
    public DaoWithCompaction() {
        super();
    }

    public DaoWithCompaction(Config conf) {
        super(conf);
    }

    @Override
    public void compact() throws IOException {
        controller.dumpIterator(new SSTableIterable(getMemTable().values(), controller, null, null));
        closeMemTable();
        controller.deleteAllOldFiles();
    }
}
