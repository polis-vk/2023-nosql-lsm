package ru.vk.itmo;

import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.util.List;

public class ReverseIteratorTest extends BaseTest {
    @DaoTest(stage = 4, maxStage = 4)
    void memTableCheckReverse(Dao<String, Entry<String>> dao) throws IOException {

        List<Entry<String>> entries = entries(100);
        for (Entry<String> entry : entries) {
            dao.upsert(entry);
        }

        assertSame(dao.get(keyAt(100), keyAt(1)),
                entries.reversed().subList(0, entries.size() - 2));
    }

    @DaoTest(stage = 4, maxStage = 4)
    void memTableCheckOrdinary(Dao<String, Entry<String>> dao) throws IOException {

        List<Entry<String>> entries = entries(100);
        for (Entry<String> entry : entries) {
            dao.upsert(entry);
        }

        assertSame(dao.all(), entries);
    }

    @DaoTest(stage = 4, maxStage = 4)
    void ssTablesCheckReverse(Dao<String, Entry<String>> dao) throws IOException {

        List<Entry<String>> entries = entries(100);
        for (Entry<String> entry : entries) {
            dao.upsert(entry);
        }

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        assertSame(dao.get(keyAt(100), keyAt(1)),
                entries.reversed().subList(0, entries.size() - 2));
    }

    @DaoTest(stage = 4, maxStage = 4)
    void ssTablesCheckOrdinary(Dao<String, Entry<String>> dao) throws IOException {

        List<Entry<String>> entries = entries(100);
        for (Entry<String> entry : entries) {
            dao.upsert(entry);
        }
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        assertSame(dao.all(), entries);
    }

    @DaoTest(stage = 4)
    void compactionReverse(Dao<String, Entry<String>> dao) throws IOException {
        List<Entry<String>> entries = entries(100);
        List<Entry<String>> firstHalf = entries.subList(0, 50);
        List<Entry<String>> lastHalf = entries.subList(50, 100);

        for (Entry<String> entry : firstHalf) {
            dao.upsert(entry);
        }
        dao.compact();
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        for (Entry<String> entry : lastHalf) {
            dao.upsert(entry);
        }
        assertSame(dao.get(keyAt(100), keyAt(1)),
                entries.reversed().subList(0, entries.size() - 2));

        dao.flush();
        assertSame(dao.get(keyAt(100), keyAt(1)),
                entries.reversed().subList(0, entries.size() - 2));

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get(keyAt(100), keyAt(1)),
                entries.reversed().subList(0, entries.size() - 2));


        dao.compact();
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get(keyAt(100), keyAt(1)),
                entries.reversed().subList(0, entries.size() - 2));

    }


    @DaoTest(stage = 4)
    void compactionOrdinary(Dao<String, Entry<String>> dao) throws IOException {
        List<Entry<String>> entries = entries(100);
        List<Entry<String>> firstHalf = entries.subList(0, 50);
        List<Entry<String>> lastHalf = entries.subList(50, 100);

        for (Entry<String> entry : firstHalf) {
            dao.upsert(entry);
        }
        dao.compact();
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        for (Entry<String> entry : lastHalf) {
            dao.upsert(entry);
        }
        assertSame(dao.all(), entries);

        dao.flush();
        assertSame(dao.all(), entries);

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.all(), entries);

        dao.compact();
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.all(), entries);
    }

    @DaoTest(stage = 1)
    void testOrder(Dao<String, Entry<String>> dao) {
        dao.upsert(entry("b", "b"));
        dao.upsert(entry("aa", "aa"));
        dao.upsert(entry("", ""));

        assertSame(
                dao.get("b", ""),

                entry("b", "b"),
                entry("aa", "aa")
        );
    }

    @DaoTest(stage = 1)
    void testFullRange(Dao<String, Entry<String>> dao) {
        dao.upsert(entry("a", "b"));

        dao.upsert(entry("b", "b"));
        dao.upsert(entry("c", "c"));

        assertSame(
                dao.get("z", ""),

                entry("c", "c"),
                entry("b", "b"),
                entry("a", "b")
        );
    }
}
