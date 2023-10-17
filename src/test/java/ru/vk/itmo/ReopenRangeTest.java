package ru.vk.itmo;


import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

public class ReopenRangeTest extends BaseTest {
    @DaoTest(stage = 3)
    void onlyPersistence(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("k1", "1"));
        dao.upsert(entry("k3", "3"));
        dao.upsert(entry("k2", "2"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("k3", "5"));
        dao.upsert(entry("k2", "4"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);

        assertSame(dao.all(),
                entry("k1", "1"),
                entry("k2", "4"),
                entry("k3", "5"));
    }

    @DaoTest(stage = 3)
    void onlyInMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("k2", "2"));
        dao.upsert(entry("k3", "3"));
        dao.upsert(entry("k1", "1"));

        assertSame(dao.all(),
                entry("k1", "1"),
                entry("k2", "2"),
                entry("k3", "3"));
    }

    @DaoTest(stage = 3)
    void withInMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("k1", "1"));
        dao.upsert(entry("k3", "3"));
        dao.upsert(entry("k2", "2"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("k3", "5"));
        dao.upsert(entry("k2", "4"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("k3", "6"));
        dao.upsert(entry("k4", "7"));

        assertSame(dao.all(),
                entry("k1", "1"),
                entry("k2", "4"),
                entry("k3", "6"),
                entry("k4", "7"));
    }

    @DaoTest(stage = 3)
    void allFromRange(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("k1", "1"));
        dao.upsert(entry("k3", "3"));
        dao.upsert(entry("k2", "2"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("k3", "5"));
        dao.upsert(entry("k2", "4"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("k3", "6"));
        dao.upsert(entry("k4", "7"));

        assertSame(dao.allFrom("k2"),
                entry("k2", "4"),
                entry("k3", "6"),
                entry("k4", "7"));
    }

    @DaoTest(stage = 3)
    void allToRange(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("k1", "1"));
        dao.upsert(entry("k3", "3"));
        dao.upsert(entry("k2", "2"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("k3", "5"));
        dao.upsert(entry("k2", "4"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("k3", "6"));
        dao.upsert(entry("k4", "7"));

        assertSame(dao.allTo("k4"),
                entry("k1", "1"),
                entry("k2", "4"),
                entry("k3", "6"));
    }

    @DaoTest(stage = 3)
    void rangeInTheMiddle(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("k1", "1"));
        dao.upsert(entry("k3", "3"));
        dao.upsert(entry("k2", "2"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("k3", "5"));
        dao.upsert(entry("k2", "4"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("k3", "6"));
        dao.upsert(entry("k4", "7"));

        assertSame(dao.get("k2", "k4"),
                entry("k2", "4"),
                entry("k3", "6"));
    }

    @DaoTest(stage = 3)
    void testDifferentFiles(Dao<String, Entry<String>> dao) throws IOException {
        List<Entry<String>> entries = entries(10_000);
        entries.forEach(dao::upsert);
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        entries = entries(10_000, 5_000);
        entries.forEach(dao::upsert);
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);

        List<Entry<String>> entries2 = new ArrayList<>(entries(5_000));
        entries2.addAll(entries);
        assertSame(dao.all(), entries2);
    }

    @DaoTest(stage = 3)
    void testManyFiles(Dao<String, Entry<String>> dao) throws IOException {
        int entriesPerFile = 1000;
        int numberOfFiles = 20;
        List<Entry<String>> entries;
        for (int i = 0; i < numberOfFiles / 2; i++) {
            entries = entries("k", "old", entriesPerFile, i * entriesPerFile);
            entries.forEach(dao::upsert);
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }

        for (int i = 0; i < numberOfFiles / 2; i++) {
            entries = entries("k", "new", entriesPerFile, i * entriesPerFile);
            entries.forEach(dao::upsert);
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }

        entries = entries("k", "new", entriesPerFile * numberOfFiles / 2);
        assertSame(dao.all(), entries);
    }

    public List<Entry<String>> entries(String keyPrefix, String valuePrefix, int count, int offset) {
        return new AbstractList<>() {
            @Override
            public Entry<String> get(int index) {
                checkInterrupted();
                if (index >= count || index < 0) {
                    throw new IndexOutOfBoundsException("Index is " + (index + offset) + ", size is " + count);
                }
                String paddedIdx = String.format("%010d", index + offset);
                String value = String.format("%010d", index + offset);
                return new BaseEntry<>(keyPrefix + paddedIdx, valuePrefix + value);
            }

            @Override
            public int size() {
                return count;
            }
        };
    }

    public List<Entry<String>> entries(int count, int offset) {
        return entries("k", "v", count, offset);
    }
}
