package ru.vk.itmo;

import org.junit.jupiter.api.Timeout;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BloomFilterBenchmarkTest extends BaseTest {
    @DaoTest(stage = 6)
    @Timeout(100500)
    public void testBenchmarkBloomFilter(Dao<String, Entry<String>> dao) throws Exception {
        final int entries = 50_000;
        final int sstables = 50; //how many sstables dao must create
        final int flushEntries = entries / sstables;  //how many entries in one sstable

        //many flushes
        manyFlushes(dao, entries, flushEntries);

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        //get average execution time
        double avgMillis = benchmarkGetFromSstable(dao, entries, true);
        printStats(avgMillis, sstables, flushEntries, true);
        avgMillis = benchmarkGetFromSstable(dao, entries, false);
        printStats(avgMillis, sstables, flushEntries, false);

    }

    private double benchmarkGetFromSstable(Dao<String, Entry<String>> dao, int entries, boolean bloomFilter) {
        //random iterating
        int[] randomEntries = randomEntries(entries);

        //benchmark 100 times for fair statistics
        final int benchmarkRounds = 50;

        List<Long> millis = new ArrayList<>();

        int warmupIters = 3;
        for (int round = 0; round < benchmarkRounds; round++) {

            long duration = 0;
            //get random entries from sstables
            for (int i = 0; i < entries; i++) {
                int randomEntry = randomEntries[i];

                long startTime = System.nanoTime();
                //get unused random entry from sstable
                Entry<String> find;
                if (bloomFilter) {
                    find = dao.get(keyAt(randomEntry));
                } else {
                    find = dao.getNoBloomFilter(keyAt(randomEntry));
                }
                long endTime = System.nanoTime();
                duration += (endTime - startTime);
                assertSame(find, entryAt(randomEntry));
            }

            if (round <= warmupIters) {
                continue;
            }

            millis.add(duration);
        }

       return avg(millis);
    }

    /**
     * @param entries size
     */
    private int[] randomEntries(int entries) {
        int[] result = new int[entries];

        Random rand = new Random();
        boolean[] usedEntries = new boolean[entries];

        for (int entry = 0; entry < entries; entry++) {
            int randomEntry;

            do {
                randomEntry = rand.nextInt(entries);
            } while (usedEntries[randomEntry]);

            usedEntries[randomEntry] = true;
            result[entry] = randomEntry;
        }

        return result;
    }

    /**
     * @param entries entries in one sstable
     * @param flush   how many sstables
     */
    private void manyFlushes(Dao<String, Entry<String>> dao, long entries, long flush) throws IOException {
        for (int entry = 0; entry < entries; entry++) {
            dao.upsert(entry(keyAt(entry), valueAt(entry)));
            if (entry % flush == 0) {
                dao.close();
                dao = DaoFactory.Factory.reopen(dao);
            }
        }
    }

    private double avg(List<Long> longs) {
        long sum = 0;

        for (long i : longs) {
            sum += i;
        }

        return (double) sum / longs.size();
    }

    private void printStats(double avgMillis, long sstables, long flushEntries, boolean bloomFilter) {
        System.out.println("AVG time: " + avgMillis + " nanos for " + sstables + " sstables with " + flushEntries + " entries." + " Bloom filter: " + bloomFilter);
    }
}
