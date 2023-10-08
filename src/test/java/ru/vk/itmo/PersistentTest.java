package ru.mail.polis;

import org.junit.jupiter.api.Assertions;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PersistentTest extends BaseTest {

    @DaoTest(stage = 2)
    void persistent(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get(keyAt(1)), entryAt(1));
    }

    @DaoTest(stage = 2)
    void multiLine(Dao<String, Entry<String>> dao) throws IOException {
        final Entry<String> entry = entry("key1\nkey2", "value1\nvalue2");
        dao.upsert(entry);
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get(entry.key()), entry);
    }

    @DaoTest(stage = 2)
    void variability(Dao<String, Entry<String>> dao) throws IOException {
        final Collection<Entry<String>> entries =
                List.of(
                        entry("key1", "value1"),
                        entry("key10", "value10"),
                        entry("key1000", "value1000"));
        entries.forEach(dao::upsert);
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        for (final Entry<String> entry : entries) {
            assertSame(dao.get(entry.key()), entry);
        }
    }

    @DaoTest(stage = 2)
    void cleanup(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.close();

        cleanUpPersistentData(dao);
        dao = DaoFactory.Factory.reopen(dao);

        Assertions.assertNull(dao.get(keyAt(1)));
    }

    @DaoTest(stage = 2)
    void persistentPreventInMemoryStorage(Dao<String, Entry<String>> dao) throws IOException {
        int keys = 175_000;
        int entityIndex = keys / 2 - 7;

        // Fill
        List<Entry<String>> entries = entries(keys);
        entries.forEach(dao::upsert);
        dao.close();

        // Materialize to consume heap
        List<Entry<String>> tmp = new ArrayList<>(entries);

        assertValueAt(DaoFactory.Factory.reopen(dao), entityIndex);

        assertSame(
                tmp.get(entityIndex),
                entries.get(entityIndex)
        );
    }

    @DaoTest(stage = 2)
    void replaceWithClose(Dao<String, Entry<String>> dao) throws IOException {
        String key = "key";
        Entry<String> e1 = entry(key, "value1");
        Entry<String> e2 = entry(key, "value2");

        // Initial insert
        try (Dao<String, Entry<String>> dao1 = dao) {
            dao1.upsert(e1);

            assertSame(dao1.get(key), e1);
        }

        // Reopen and replace
        try (Dao<String, Entry<String>> dao2 = DaoFactory.Factory.reopen(dao)) {
            assertSame(dao2.get(key), e1);
            dao2.upsert(e2);
            assertSame(dao2.get(key), e2);
        }

        // Reopen and check
        try (Dao<String, Entry<String>> dao3 = DaoFactory.Factory.reopen(dao)) {
            assertSame(dao3.get(key), e2);
        }
    }

}
