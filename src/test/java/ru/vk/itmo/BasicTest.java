package ru.mail.polis;

import org.junit.jupiter.api.Assertions;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author incubos
 */
public class BasicTest extends BaseTest {

    @DaoTest(stage = 1)
    void testEmpty(Dao<String, Entry<String>> dao) throws IOException {
        assertEmpty(dao.all());
    }

    @DaoTest(stage = 1)
    void testSingle(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("a", "b"));
        assertSame(
                dao.all(),
                entry("a", "b")
        );
    }

    @DaoTest(stage = 1)
    void testTree(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("e", "f"));
        dao.upsert(entry("c", "d"));
        dao.upsert(entry("a", "b"));

        assertSame(
                dao.all(),

                entry("a", "b"),
                entry("c", "d"),
                entry("e", "f")
        );
    }

    @DaoTest(stage = 1)
    void testManyIterators(Dao<String, Entry<String>> dao) throws Exception {
        List<Entry<String>> entries = new ArrayList<>(entries(10_000));
        for (Entry<String> entry : entries) {
            dao.upsert(entry);
        }
        try {
            List<Iterator<Entry<String>>> iterators = new ArrayList<>();
            for (int i = 0; i < 10_000; i++) {
                iterators.add(dao.all());
            }
            // just utilize the collection
            Assertions.assertEquals(10_000, iterators.size());
        } catch (OutOfMemoryError error) {
            throw new AssertionFailedError("Too much data in memory: use some lazy ways", error);
        }
    }

    @DaoTest(stage = 1)
    void testFindValueInTheMiddle(Dao<String, Entry<String>> dao) throws Exception {
        dao.upsert(entry("e", "f"));
        dao.upsert(entry("c", "d"));
        dao.upsert(entry("a", "b"));

        assertSame(dao.get("c"), entry("c", "d"));
    }

    @DaoTest(stage = 1)
    void testFindRangeInTheMiddle(Dao<String, Entry<String>> dao) throws Exception {
        dao.upsert(entry("e", "f"));
        dao.upsert(entry("c", "d"));
        dao.upsert(entry("a", "b"));

        assertSame(dao.get("c", "e"), entry("c", "d"));
    }

    @DaoTest(stage = 1)
    void testFindFullRange(Dao<String, Entry<String>> dao) throws Exception {
        dao.upsert(entry("e", "f"));
        dao.upsert(entry("c", "d"));
        dao.upsert(entry("a", "b"));


        assertSame(
                dao.get("a", "z"),

                entry("a", "b"),
                entry("c", "d"),
                entry("e", "f")
        );
    }

    @DaoTest(stage = 1)
    void testAllTo(Dao<String, Entry<String>> dao) throws Exception {
        dao.upsert(entry("e", "f"));
        dao.upsert(entry("c", "d"));
        dao.upsert(entry("a", "b"));


        assertSame(
                dao.allTo("e"),

                entry("a", "b"),
                entry("c", "d")
        );
    }

    @DaoTest(stage = 1)
    void testHugeData(Dao<String, Entry<String>> dao) throws Exception {
        int count = 100_000;
        entries(count).forEach(dao::upsert);

        for (int i = 0; i < count; i++) {
            assertSame(dao.get(keyAt(i)), entryAt(i));
        }
    }


}
