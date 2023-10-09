package ru.vk.itmo;


import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class PersistentRangeTest extends BaseTest {
    public static final int[] NOTHING = new int[0];
    private static final int[] DATASET = new int[]{7, 97, 101};

    private void sliceAndDice(Dao<String, Entry<String>> dao) throws IOException {
        // Full
        assertSame(dao.all(), DATASET);

        // From
        assertSame(dao.allFrom(keyAt(6)), DATASET);
        assertSame(dao.allFrom(keyAt(7)), DATASET);
        assertSame(dao.allFrom(keyAt(8)), 97, 101);
        assertSame(dao.allFrom(keyAt(96)), 97, 101);
        assertSame(dao.allFrom(keyAt(97)), 97, 101);
        assertSame(dao.allFrom(keyAt(98)), 101);
        assertSame(dao.allFrom(keyAt(100)), 101);
        assertSame(dao.allFrom(keyAt(101)), 101);
        assertSame(dao.allFrom(keyAt(102)), NOTHING);

        // Right
        assertSame(dao.allTo(keyAt(102)), DATASET);
        assertSame(dao.allTo(keyAt(101)), 7, 97);
        assertSame(dao.allTo(keyAt(100)), 7, 97);
        assertSame(dao.allTo(keyAt(98)), 7, 97);
        assertSame(dao.allTo(keyAt(97)), 7);
        assertSame(dao.allTo(keyAt(96)), 7);
        assertSame(dao.allTo(keyAt(8)), 7);
        assertSame(dao.allTo(keyAt(7)), NOTHING);
        assertSame(dao.allTo(keyAt(6)), NOTHING);

        // Between

        assertSame(dao.get(keyAt(6), keyAt(102)), DATASET);
        assertSame(dao.get(keyAt(7), keyAt(102)), DATASET);

        assertSame(dao.get(keyAt(6), keyAt(101)), 7, 97);
        assertSame(dao.get(keyAt(7), keyAt(101)), 7, 97);
        assertSame(dao.get(keyAt(7), keyAt(98)), 7, 97);

        assertSame(dao.get(keyAt(7), keyAt(97)), 7);
        assertSame(dao.get(keyAt(6), keyAt(97)), 7);
        assertSame(dao.get(keyAt(6), keyAt(96)), 7);
        assertSame(dao.get(keyAt(6), keyAt(8)), 7);
        assertSame(dao.get(keyAt(7), keyAt(7)), NOTHING);

        assertSame(dao.get(keyAt(97), keyAt(102)), 97, 101);
        assertSame(dao.get(keyAt(96), keyAt(102)), 97, 101);
        assertSame(dao.get(keyAt(98), keyAt(102)), 101);
        assertSame(dao.get(keyAt(98), keyAt(101)), NOTHING);
        assertSame(dao.get(keyAt(98), keyAt(100)), NOTHING);
        assertSame(dao.get(keyAt(102), keyAt(1000)), NOTHING);
        assertSame(dao.get(keyAt(0), keyAt(7)), NOTHING);
        assertSame(dao.get(keyAt(0), keyAt(6)), NOTHING);
    }

    @DaoTest(stage = 3)
    void justMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(101));
        dao.upsert(entryAt(97));
        dao.upsert(entryAt(7));

        sliceAndDice(dao);
    }

    @DaoTest(stage = 3)
    void justDisk(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(97));
        dao.upsert(entryAt(7));
        dao.upsert(entryAt(101));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        sliceAndDice(dao);
    }

    @DaoTest(stage = 3)
    void justDisk2(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(101));
        dao.upsert(entryAt(97));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entryAt(7));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        sliceAndDice(dao);
    }

    @DaoTest(stage = 3)
    void mixedMemoryDisk(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(7));
        dao.upsert(entryAt(97));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entryAt(101));

        sliceAndDice(dao);
    }

    @DaoTest(stage = 3)
    void mixedMemoryDisk2(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(7));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entryAt(97));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entryAt(101));

        sliceAndDice(dao);
    }

    @DaoTest(stage = 3)
    void replaceWithMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(7));
        dao.upsert(entry(keyAt(97), "old97"));
        dao.upsert(entryAt(101));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entryAt(97));

        sliceAndDice(dao);
    }

    @DaoTest(stage = 3)
    void replaceOnDisk(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(7));
        dao.upsert(entry(keyAt(97), "old97"));
        dao.upsert(entryAt(101));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entryAt(97));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        sliceAndDice(dao);
    }

    @DaoTest(stage = 3)
    void fresh(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(7), "old7"));
        dao.upsert(entryAt(97));
        dao.upsert(entry(keyAt(101), "old101"));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entryAt(7));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entryAt(101));

        sliceAndDice(dao);
    }

    @DaoTest(stage = 3)
    void concat(Dao<String, Entry<String>> dao) throws IOException {
        final int flushes = 10;
        final int entries = 1000;
        final int step = 7;

        int i = 0;
        for (int flush = 0; flush < flushes; flush++) {
            for (int entry = 0; entry < entries; entry++) {
                dao.upsert(entryAt(i));
                i += step;
            }
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }

        final Iterator<Entry<String>> all = dao.all();
        int expected = 0;
        while (i > 0) {
            assertTrue(all.hasNext());
            assertSame(all.next(), entryAt(expected));
            expected += step;
            i -= step;
        }
    }

    @DaoTest(stage = 3)
    void interleave(Dao<String, Entry<String>> dao) throws IOException {
        final int flushes = 10;
        final int entries = 1000;

        for (int flush = 0; flush < flushes; flush++) {
            int i = flush;
            for (int entry = 0; entry < entries; entry++) {
                dao.upsert(entryAt(i));
                i += flushes;
            }
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }

        final Iterator<Entry<String>> all = dao.all();
        final int limit = flushes * entries;
        int expected = 0;
        while (expected < limit) {
            assertTrue(all.hasNext());
            assertSame(all.next(), entryAt(expected));
            expected++;
        }
    }

    @DaoTest(stage = 3)
    void overwrite(Dao<String, Entry<String>> dao) throws IOException {
        final int flushes = 10;
        final int entries = 1000;

        for (int flush = 0; flush < flushes; flush++) {
            for (int entry = 0; entry < entries; entry++) {
                dao.upsert(entryAt(entry));
            }
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }

        final Iterator<Entry<String>> all = dao.all();
        for (int entry = 0; entry < entries; entry++) {
            assertTrue(all.hasNext());
            assertSame(all.next(), entryAt(entry));
        }
    }
}
