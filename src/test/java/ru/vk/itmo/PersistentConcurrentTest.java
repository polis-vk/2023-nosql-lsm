package ru.vk.itmo;

import java.io.IOException;
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

    @DaoTest(stage = 5)
    void testConcurrentRW_100_000_compact(Dao<String, Entry<String>> dao) throws Exception {
        int count = 100_000;
        List<Entry<String>> entries = entries("k", "v", count);
        runInParallel(100, count, value -> {
            tryRun(() -> dao.upsert(entries.get(value)));
            tryRun(() -> dao.upsert(entry(keyAt(value), null)));
            tryRun(() -> dao.upsert(entries.get(value)));
        }, () -> {
            for(int i = 0; i < 100; i++) {
                try {
                    dao.compact();
                    dao.flush();
                    Thread.sleep(30);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).close();
        dao.close();

        Dao<String, Entry<String>> dao2 = DaoFactory.Factory.reopen(dao);
        runInParallel(100, count, value -> {
            assertSame(dao2.get(entries.get(value).key()), entries.get(value));
        }).close();
    }

    public void tryRun(Runnable runnable) throws InterruptedException {
        while(true) {
            try {
                runnable.run();
                return;
            } catch (Exception e) {
                //noinspection BusyWait
                Thread.sleep(100);
            }
        }
    }
}
