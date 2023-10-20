package ru.vk.itmo;

import java.util.List;

import ru.vk.itmo.test.DaoFactory;

/**
 * @author andrey.timofeev
 */
public class PersistentConcurrentTest extends BaseTest {
    @DaoTest(stage = 4)
    void testConcurrentRW_2_500_2(Dao<String, Entry<String>> dao) throws Exception {
        int count = 2_500;
        List<Entry<String>> entries = entries("k", "v", count);
        runInParallel(100, count, value -> {
            dao.upsert(entries.get(value));
        }).close();
        dao.close();

        Dao<String, Entry<String>> dao2 = DaoFactory.Factory.reopen(dao);
        runInParallel(100, count, value -> {
            assertSame(dao2.get(entries.get(value).key()), entries.get(value));
        }).close();
    }
}
