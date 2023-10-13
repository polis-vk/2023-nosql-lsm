package ru.vk.itmo;

import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class PersistentDeletionTest extends BaseTest {

    @DaoTest(stage = 3)
    void deleteOldValueFromFile(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), "removable"));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entry(keyAt(1), null));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        assertNull(dao.get(keyAt(1)));
        assertFalse(dao.allFrom(keyAt(1)).hasNext());
    }

    @DaoTest(stage = 3)
    void deleteOldValueFromMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), "removable"));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entry(keyAt(1), null));

        assertNull(dao.get(keyAt(1)));
        assertFalse(dao.allFrom(keyAt(1)).hasNext());
    }

    @DaoTest(stage = 3)
    void deleteFromMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), "removable"));
        dao.upsert(entry(keyAt(1), null));

        assertNull(dao.get(keyAt(1)));
        assertFalse(dao.allFrom(keyAt(1)).hasNext());
    }

    @DaoTest(stage = 3)
    void deleteFromDisk(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), "removable"));
        dao.upsert(entry(keyAt(1), null));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        assertNull(dao.get(keyAt(1)));
        assertFalse(dao.allFrom(keyAt(1)).hasNext());
    }

    @DaoTest(stage = 3)
    void restoreOldValueInFile(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), "removable"));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entry(keyAt(1), null));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entryAt(1));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        assertSame(dao.all(), 1);
    }

    @DaoTest(stage = 3)
    void restoreOldValueInMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), "removable"));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entry(keyAt(1), null));

        dao.upsert(entryAt(1));

        assertSame(dao.all(), 1);
    }

    @DaoTest(stage = 3)
    void restoreInMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), "removable"));
        dao.upsert(entry(keyAt(1), null));
        dao.upsert(entryAt(1));

        assertSame(dao.all(), 1);
    }

    @DaoTest(stage = 3)
    void restoreOnDisk(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), "removable"));
        dao.upsert(entry(keyAt(1), null));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entryAt(1));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        assertSame(dao.all(), 1);
    }

    @DaoTest(stage = 3)
    void upsertNonExistent(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), null));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        assertFalse(dao.all().hasNext());
    }

    @DaoTest(stage = 3)
    void emptyStringIsNotNull(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), ""));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        assertSame(dao.get(keyAt(1)), entry(keyAt(1), ""));
    }

    @DaoTest(stage = 3)
    void rangeStressTest(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        for (int i = 2; i < 10; i++) {
            dao.upsert(entry(keyAt(i), null));
        }
        dao.upsert(entryAt(10));

        assertFalse(dao.get(keyAt(2), keyAt(10)).hasNext());
        assertSame(dao.allTo(keyAt(10)), 1);
        assertSame(dao.allFrom(keyAt(2)), 10);

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        assertFalse(dao.get(keyAt(2), keyAt(10)).hasNext());
        assertSame(dao.allTo(keyAt(10)), 1);
        assertSame(dao.allFrom(keyAt(2)), 10);

        dao.upsert(entryAt(5));

        assertSame(dao.get(keyAt(2), keyAt(10)), 5);
        assertSame(dao.allTo(keyAt(10)), 1, 5);
        assertSame(dao.allFrom(keyAt(2)), 5, 10);

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        assertSame(dao.get(keyAt(2), keyAt(10)), 5);
        assertSame(dao.allTo(keyAt(10)), 1, 5);
        assertSame(dao.allFrom(keyAt(2)), 5, 10);
    }

    @DaoTest(stage = 3)
    void checkFlow(Dao<String, Entry<String>> dao) throws IOException {
        NavigableSet<Entry<String>> values = new TreeSet<>(Comparator.comparing(Entry::key));
        for (int i = 1; i <= 100; i++) {
            values.add(entryAt(i));
            dao.upsert(entryAt(i));
        }

        for (int i = 75; i <= 90; i++) {
            removeValue(i, dao, values);
        }

        performChecks(dao, values);

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        for (int i = 30; i <= 60; i++) {
            removeValue(i, dao, values);
        }

        performChecks(dao, values);

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        for (int i = 5; i <= 15; i++) {
            removeValue(i, dao, values);
        }

        performChecks(dao, values);

        for (int i = 45; i <= 60; i++) {
            addValue(i, dao, values);
        }

        performChecks(dao, values);

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        performChecks(dao, values);
    }

    void performChecks(Dao<String, Entry<String>> dao, NavigableSet<Entry<String>> values) throws IOException {
        assertSame(dao.all(), List.copyOf(values));
        int oneForth = values.size() / 4;
        for (int i = 1; i < 4; i++) {
            assertSame(dao.allFrom(keyAt(i * oneForth)),
                    List.copyOf(values.tailSet(entryAt(i * oneForth))));
            assertSame(dao.allTo(keyAt(i * oneForth)),
                    List.copyOf(values.headSet(entryAt(i * oneForth))));
        }
        int size = values.size();
        assertSame(dao.get(keyAt(size / 4), keyAt(size - size / 4)),
                List.copyOf(values.subSet(entryAt(size / 4), entryAt(size - size / 4))));
    }

    void removeValue(int value, Dao<String, Entry<String>> dao, NavigableSet<Entry<String>> values) {
        dao.upsert(entry(keyAt(value), null));
        values.remove(entryAt(value));
    }

    void addValue(int value, Dao<String, Entry<String>> dao, NavigableSet<Entry<String>> values) {
        dao.upsert(entryAt(value));
        values.add(entryAt(value));
    }
}
