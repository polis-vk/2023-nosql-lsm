package ru.vk.itmo;


import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;

public class IteratorTest extends BaseTest {

    @SafeVarargs
    final void parts(final Dao<String, Entry<String>> dao, final List<Entry<String>>... parts) throws Exception {
        for (final List<Entry<String>> list: parts) {
            for (final Entry<String> entry: list) {
                dao.upsert(entry);
            }
            dao.flush();
            Thread.sleep(50);
        }
    }

    @DaoTest(stage = 5)
    void testFlush(final Dao<String, Entry<String>> dao) throws Exception {
        final int totalCount = 10;
        List<Entry<String>> entries = entries(totalCount);
        List<Entry<String>> firstHalf = entries.subList(0, totalCount / 2);
        List<Entry<String>> secondHalf = entries.subList(totalCount / 2, totalCount);

        parts(dao, firstHalf);
        final Iterator<Entry<String>> iterator = dao.get(keyAt(0), keyAt(totalCount / 2));
        parts(dao, secondHalf);

        assertSame(iterator, firstHalf);
    }

    @DaoTest(stage = 5)
    void testCompactEmpty(final Dao<String, Entry<String>> dao) throws Exception {
        final int totalCount = 10;
        List<Entry<String>> entries = entries(totalCount);
        parts(dao, entries);
        final Iterator<Entry<String>> iterator = dao.get(keyAt(0), keyAt(totalCount));
        parts(dao, entries.stream().map(it -> entry(it.key(), null)).toList());

        dao.compact();
        Thread.sleep(50);

        while (iterator.hasNext()) {
            iterator.next();
        }
    }

    void testCompact(final Dao<String, Entry<String>> dao, final boolean waitCompaction) throws Exception {
        final int totalCount = 100;
        List<Entry<String>> entries = entries(totalCount);
        parts(dao, entries.subList(0, totalCount / 2), entries.subList(totalCount / 2, totalCount));
        final Iterator<Entry<String>> iterator = dao.all();
        dao.compact();
        if (waitCompaction) {
            Thread.sleep(50);
        }
        assertSame(iterator, entries);
    }

    @DaoTest(stage = 5)
    void testCompact(final Dao<String, Entry<String>> dao) throws Exception {
        testCompact(dao, true);
    }

    @DaoTest(stage = 5)
    void testCompactWhileReading(final Dao<String, Entry<String>> dao) throws Exception {
        testCompact(dao, false);
    }

    void testCompactMany(final Dao<String, Entry<String>> dao, final boolean waitCompaction) throws Exception {
        final int totalCount = 3000;
        List<Entry<String>> entries = entries(totalCount);
        final int partCount = 50;
        @SuppressWarnings("unchecked")
        final List<Entry<String>>[] entriesParts = (List<Entry<String>>[]) Array.newInstance(List.class, partCount);
        for (int i = 0; i < partCount; i++) {
            entriesParts[i] = entries.subList(totalCount / partCount * i, totalCount / partCount * (i + 1));
        }
        parts(dao, entriesParts);

        final Iterator<Entry<String>> iterator = dao.all();
        dao.compact();
        if (waitCompaction) {
            Thread.sleep(100);
        }
        assertSame(iterator, entries);
    }

    @DaoTest(stage = 5)
    void testCompactMany(final Dao<String, Entry<String>> dao) throws Exception {
        testCompactMany(dao, true);
    }

    @DaoTest(stage = 5)
    void testCompactManyWhileReading(final Dao<String, Entry<String>> dao) throws Exception {
        testCompactMany(dao, false);
    }


}
