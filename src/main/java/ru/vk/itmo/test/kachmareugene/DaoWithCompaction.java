package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Config;
import java.io.IOException;

// TODO убрать поиск за лог
// TODO убрать дополнения в виде дат
// TODO считать отдельно длину файла и memtable - ошибка
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
