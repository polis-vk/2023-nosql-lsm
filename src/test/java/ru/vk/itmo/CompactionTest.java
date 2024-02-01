package ru.vk.itmo;

import org.junit.jupiter.api.Timeout;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.NavigableSet;
import java.util.TreeSet;

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
    @Timeout(value = 20)
    void overwrite(Dao<String, Entry<String>> dao) throws Exception {
        // Reference value
        int valueSize = 10 * 1024 * 1024;
        int keyCount = 3;
        int overwrites = 5;

        // 1 second should be enough to flush 10MB even to HDD
        Duration flushDelay = Duration.ofSeconds(1);

        List<Entry<String>> entries = bigValues(keyCount, valueSize);

        // Overwrite keys several times each time closing DAO
        for (int round = 0; round < overwrites; round++) {
            for (Entry<String> entry : entries) {
                dao.upsert(entry);

                // Wait for a possible auto flush from stage 5
                Thread.sleep(flushDelay);
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
        System.out.println(smallSize);
        System.out.println(bigSize);

        // Heuristic
        assertTrue(smallSize * (overwrites - 1) < bigSize);
        assertTrue(smallSize * (overwrites + 1) > bigSize);
    }

    @DaoTest(stage = 4, maxStage = 4)
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

    @DaoTest(stage = 5)
    void removeAllAndCompact(Dao<String, Entry<String>> dao) throws IOException {
        List<Entry<String>> entries = entries(100);
        for (Entry<String> entry : entries) {
            dao.upsert(entry);
        }
        dao.flush();
        assertSame(dao.all(), entries);
        dao.compact();
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.all(), entries);

        // remove all
        for (int i = 0; i < entries.size(); i++) {
            dao.upsert(entry(keyAt(i), null));
        }
        // before compaction
        assertSame(dao.all(), new int[0]);
        // after flushing on disk
        dao.flush();
        assertSame(dao.all(), new int[0]);

        dao.compact();
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        // after compaction
        assertSame(dao.all(), new int[0]);
    }

    @DaoTest(stage = 5)
    void mixedCompact(Dao<String, Entry<String>> dao) throws IOException {
        NavigableSet<Entry<String>> values = new TreeSet<>(Comparator.comparing(Entry::key));
        // insert some entries
        for (int i = 0; i < 50; i++) {
            values.add(entryAt(i));
            dao.upsert(entryAt(i));
        }

        // remove some entries
        for (int i = 0; i < 25; i++) {
            dao.upsert(entry(keyAt(i), null));
            values.remove(entryAt(i));
        }

        // insert more entries
        for (int i = 50; i < 100; i++) {
            values.add(entryAt(i));
            dao.upsert(entryAt(i));
        }

        assertSame(dao.all(), List.copyOf(values));

        dao.flush();
        assertSame(dao.all(), List.copyOf(values));

        dao.compact();
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);

        assertSame(dao.all(), List.copyOf(values));
    }

    @DaoTest(stage = 5)
    void addRemoveAddAndCompact(Dao<String, Entry<String>> dao) throws IOException {
        NavigableSet<Entry<String>> values = new TreeSet<>(Comparator.comparing(Entry::key));
        // insert some entries
        for (int i = 0; i < 50; i++) {
            values.add(entryAt(i));
            dao.upsert(entryAt(i));
        }

        // remove some entries
        for (int i = 0; i < 25; i++) {
            dao.upsert(entry(keyAt(i), null));
            values.remove(entryAt(i));
        }

        assertSame(dao.all(), List.copyOf(values));

        // flush and check
        dao.flush();
        assertSame(dao.all(), List.copyOf(values));

        // re-insert entries
        for (int i = 0; i < 25; i++) {
            values.add(entryAt(i));
            dao.upsert(entryAt(i));
        }

        assertSame(dao.all(), List.copyOf(values));

        // flush and check
        dao.flush();
        assertSame(dao.all(), List.copyOf(values));

        // compact and check
        dao.compact();
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);

        assertSame(dao.all(), List.copyOf(values));
    }

}
