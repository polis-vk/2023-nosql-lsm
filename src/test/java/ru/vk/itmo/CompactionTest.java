package ru.vk.itmo;

import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compaction tests for {@link Dao} implementations.
 *
 * @author Vadim Tsesko
 */
class CompactionTest extends BaseTest {
    @DaoTest(stage = 4)
    void empty(Dao<String, Entry<String>> dao) throws IOException {
        // Compact
        dao.compact();
        dao.close();

        // Check the contents
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.all(), new int[0]);
    }

    @DaoTest(stage = 4)
    void nothingToFlush(Dao<String, Entry<String>> dao) throws IOException {
        final Entry<String> entry = entryAt(42);
        dao.upsert(entry);

        // Compact and flush
        dao.compact();
        dao.close();

        // Check the contents
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.all(), entry);

        // Compact and flush
        dao.compact();
        dao.close();

        // Check the contents
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.all(), entry);
    }

    @DaoTest(stage = 4)
    void overwrite(Dao<String, Entry<String>> dao) throws IOException {
        // Reference value
        int valueSize = 1024 * 1024;
        int keyCount = 10;
        int overwrites = 10;

        List<Entry<String>> entries = bigValues(keyCount, valueSize);

        // Overwrite keys several times each time closing DAO
        for (int round = 0; round < overwrites; round++) {
            for (Entry<String> entry : entries) {
                dao.upsert(entry);
            }
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }

        // Big size
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        long bigSize = sizePersistentData(dao);

        // Compact
        dao.compact();
        dao.close();

        // Check the contents
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.all(), entries);

        // Check store size
        long smallSize = sizePersistentData(dao);

        // Heuristic
        assertTrue(smallSize * (overwrites - 1) < bigSize);
        assertTrue(smallSize * (overwrites + 1) > bigSize);
    }

    @DaoTest(stage = 4)
    void multiple(Dao<String, Entry<String>> dao) throws IOException {
        // Reference value
        int valueSize = 1024 * 1024;
        int keyCount = 10;
        int overwrites = 10;

        List<Entry<String>> entries = bigValues(keyCount, valueSize);
        List<Long> sizes = new ArrayList<>();

        // Overwrite keys multiple times with intermediate compactions
        for (int round = 0; round < overwrites; round++) {
            // Overwrite
            for (Entry<String> entry : entries) {
                dao.upsert(entry);
            }

            // Compact
            dao.compact();
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
            sizes.add(sizePersistentData(dao));
        }

        LongSummaryStatistics stats = sizes.stream().mapToLong(k -> k).summaryStatistics();
        // Heuristic
        assertTrue(stats.getMax() - stats.getMin() < 1024);
    }

    @DaoTest(stage = 4)
    void compactAndAdd(Dao<String, Entry<String>> dao) throws IOException {
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

}
