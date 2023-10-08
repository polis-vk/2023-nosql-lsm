package ru.vk.itmo;

import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;

public class UpsertRemoveTest extends BaseTest {

    @DaoTest(stage = 3)
    void persistentRemoveTest(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry(keyAt(1), null));
        assertSame(dao.get(keyAt(1)), null);
    }

    @DaoTest(stage = 3)
    void persistentRemoveTestRange(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(2));
        dao.upsert(entry(keyAt(2), null));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);

        assertSame(dao.all(), entryAt(1));
    }

    @DaoTest(stage = 3)
    void persistentRemoveTestRangeInMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(2));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry(keyAt(2), null));

        assertSame(dao.all(), entryAt(1));
    }

    @DaoTest(stage = 3)
    void persistentGetAfterRemoveTestRange(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(2));
        dao.upsert(entry(keyAt(2), null));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entryAt(2));

        assertSame(dao.all(), entryAt(1), entryAt(2));
    }

    @DaoTest(stage = 3)
    void persistentGetAfterRemoveTestRangeFromMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(2));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry(keyAt(2), null));
        dao.upsert(entryAt(2));

        assertSame(dao.all(), entryAt(1), entryAt(2));
    }

    @DaoTest(stage = 3)
    void manyRemoveRecords(Dao<String, Entry<String>> dao) throws IOException {
        for (int i = 0; i < 6; i++) {
            dao.upsert(entryAt(i));
        }
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry(keyAt(1), null));
        dao.upsert(entry(keyAt(2), null));
        dao.upsert(entry(keyAt(4), null));
        dao.upsert(entry(keyAt(5), null));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry(keyAt(3), null));
        dao.upsert(entryAt(4));
        dao.upsert(entry(keyAt(5), "new value"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entryAt(2));
        dao.upsert(entryAt(1));

        assertSame(
                dao.all(),
                entryAt(0),
                entryAt(1),
                entryAt(2),
                entryAt(4),
                entry(keyAt(5), "new value")
        );
    }

}
