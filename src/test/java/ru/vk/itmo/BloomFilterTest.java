package ru.vk.itmo;

import ru.vk.itmo.bandurinvladislav.BloomFilter;
import ru.vk.itmo.bandurinvladislav.StorageUtil;
import ru.vk.itmo.test.DaoFactory;
import ru.vk.itmo.util.BloomTestUtil;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class BloomFilterTest extends BaseTest {

    @DaoTest(stage = 4)
    void checkOriginalAndFileBloom(Dao<String, Entry<String>> dao)
            throws IOException, NoSuchFieldException, IllegalAccessException, NoSuchMethodException {
        int keys = 175_000;

        // Fill and construct original bloom filter
        BloomFilter originalBloom = BloomFilter.createBloom(keys);
        List<Entry<String>> entries = entries(keys);
        entries.forEach(dao::upsert);
        entries.forEach(entry -> originalBloom.add(BloomTestUtil.fromString(entry.key())));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);

        // I really need access for private fields to test bloom properly
        List<MemorySegment> sstables = BloomTestUtil.getSegmentList(dao);

        MemorySegment flushedSegment = sstables.get(0);

        assertEquals(1, sstables.size());
        assertEquals(originalBloom.getFilterSize() * 8L,
                flushedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0) - Long.BYTES);

        // here we check if all longs in file are the same as in the original bloom_filter
        long sstableOffset = Long.BYTES;
        for (Long originalLong : originalBloom.getFilter().getLongs()) {
            long fileLong = flushedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, sstableOffset);
            assertEquals(originalLong, fileLong);
            sstableOffset += Long.BYTES;
        }

    }

    @DaoTest(stage = 4)
    void compareSetBits(Dao<String, Entry<String>> dao)
            throws IOException, NoSuchFieldException, IllegalAccessException {
        int keys = 10000;

        BloomFilter bloom = BloomFilter.createBloom(keys);
        List<Entry<String>> entries = entries(keys);
        entries.forEach(dao::upsert);
        entries.forEach(entry -> bloom.add(BloomTestUtil.fromString(entry.key())));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        List<MemorySegment> sstables = BloomTestUtil.getSegmentList(dao);

        // indexes of bits which were set before while adding into bloom filter
        List<Integer> setBitsIndexes = new ArrayList<>();

        int bloomSizeBits = bloom.getFilterSize() * 64;
        for (int i = 0; i < bloomSizeBits; i++) {
            if (bloom.getFilter().get(i)) {
                setBitsIndexes.add(i);
            }
        }

        // Check if all bits in file
        assertTrue(StorageUtil.checkIndexes(sstables.get(0),
                setBitsIndexes.stream().mapToLong(i -> (long) i).toArray()));

        // Check if no other bits are set in sstable (test for every bit)
        for (int i = 0; i < bloomSizeBits; i++) {
            if (!bloom.getFilter().get(i)) {
                assertFalse(StorageUtil.checkIndexes(sstables.get(0), new long[]{i}));
            }
        }

        // Check if every added key is actually found in bloom filter
        for (Entry<String> entry : entries) {
            assertTrue(StorageUtil.checkBloom(sstables.get(0), BloomTestUtil.fromString(entry.key())));
        }
    }

    // Check for expected false positive rate (0.01)
    @DaoTest(stage = 4)
    void falsePositiveRateTest(Dao<String, Entry<String>> dao)
            throws IOException, NoSuchFieldException, IllegalAccessException {
        int keys = 200_000;

        List<Entry<String>> entries = entries(keys);
        List<Entry<String>> entriesToUpsert = entries.subList(0, 100_000);
        BloomFilter bloom = BloomFilter.createBloom(keys / 2);

        // Insert 100_000 entries
        entriesToUpsert.forEach(entry -> bloom.add(BloomTestUtil.fromString(entry.key())));
        entriesToUpsert.forEach(dao::upsert);
        List<Entry<String>> nonExistingEntries = entries.subList(100_000, keys);
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);

        List<MemorySegment> sstables = BloomTestUtil.getSegmentList(dao);

        // Going through non-existing keys and count false positive rate
        int falsePositiveCount = 0;
        for (Entry<String> nonExistingEntry : nonExistingEntries) {
            if (StorageUtil.checkBloom(sstables.get(0), BloomTestUtil.fromString(nonExistingEntry.key()))) {
                falsePositiveCount++;
            }
        }

        assertTrue((double) falsePositiveCount / (double) 100_000 < 0.011);
    }
}
