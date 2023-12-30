package ru.vk.itmo.viktorkorotkikh;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import ru.vk.itmo.BaseTest;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.MusicTest;
import ru.vk.itmo.test.DaoFactory;
import ru.vk.itmo.test.viktorkorotkikh.FactoryImpl;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
class CompressionTest extends BaseTest {
    private static final int BLOCK_4KB = 4 * 1024;
    private static final int BLOCK_2KB = 2 * 1024;
    private static final DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> factory = new FactoryImpl();

    @Test
    void compareUncompressedAndCompressedDataSize() throws IOException {
        Path uncompressedTmp = Files.createTempDirectory("uncompressedDao");
        Path compressedTmp = Files.createTempDirectory("compressedDao");
        long flushThreshold = 1 << 20; // 1 MB

        Dao<String, Entry<String>> uncompressedDao = factory.createStringDao(new Config(
                uncompressedTmp,
                flushThreshold,
                Config.disableCompression()
        ));
        Dao<String, Entry<String>> compressedDao = factory.createStringDao(new Config(
                compressedTmp,
                flushThreshold,
                new Config.CompressionConfig(true, Config.CompressionConfig.Compressor.LZ4, BLOCK_4KB)
        ));
        List<Entry<String>> entries = entries(1000);
        entries.forEach(stringEntry -> {
            uncompressedDao.upsert(stringEntry);
            compressedDao.upsert(stringEntry);
        });
        // finish all bg processes
        uncompressedDao.close();
        compressedDao.close();

        long uncompressedSize = sizePersistentData(uncompressedDao);
        long compressedSize = sizePersistentData(compressedDao);
        assertTrue(compressedSize < uncompressedSize);
        cleanUp(compressedTmp);
        cleanUp(uncompressedTmp);
    }

    @Test
    void compressAndCheckData() throws IOException {
        Path compressedTmp = Files.createTempDirectory("compressedDao");
        long flushThreshold = 1 << 20; // 1 MB

        Dao<String, Entry<String>> compressedDao = factory.createStringDao(new Config(
                compressedTmp,
                flushThreshold,
                new Config.CompressionConfig(true, Config.CompressionConfig.Compressor.LZ4, BLOCK_4KB)
        ));
        addRemoveAddAndCompact(compressedDao);
        cleanUp(compressedTmp);
    }

    private void addRemoveAddAndCompact(Dao<String, Entry<String>> dao) throws IOException {
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

    @Test
    void compressMusicTest() throws Exception {
        Path compressedTmp = Files.createTempDirectory("compressedDao");
        long flushThreshold = 1 << 20; // 1 MB

        Dao<String, Entry<String>> compressedDao = factory.createStringDao(new Config(
                compressedTmp,
                flushThreshold,
                new Config.CompressionConfig(true, Config.CompressionConfig.Compressor.LZ4, BLOCK_4KB)
        ));
        new MusicTest().database(compressedDao);
        cleanUp(compressedTmp);
    }

    @Test
    @Timeout(20)
    void writeUncompressedReopenAndCompress() throws IOException, InterruptedException {
        Path tmp = Files.createTempDirectory("dao");
        long flushThreshold = 1 << 20; // 1 MB

        // uncompressed dao
        Dao<String, Entry<String>> dao = factory.createStringDao(new Config(
                tmp,
                flushThreshold,
                Config.disableCompression()
        ));
        int valueSize = 10 * 1024 * 1024;
        int keyCount = 3;
        List<Entry<String>> entries = bigValues(keyCount, valueSize);

        // 1 second should be enough to flush 10MB even to HDD
        Duration flushDelay = Duration.ofSeconds(1);

        for (Entry<String> entry : entries) {
            dao.upsert(entry);
            Thread.sleep(flushDelay);
        }

        dao.close();

        // compressed dao
        dao = factory.createStringDao(new Config(
                tmp,
                flushThreshold,
                new Config.CompressionConfig(true, Config.CompressionConfig.Compressor.LZ4, BLOCK_4KB)
        ));
        assertSame(dao.all(), entries);

        long uncompressedSize = sizePersistentData(dao);

        dao.compact();
        Thread.sleep(flushDelay.multipliedBy(5)); // wait for the compaction to complete

        assertSame(dao.all(), entries);

        long compressedSize = sizePersistentData(dao);
        assertTrue(compressedSize < uncompressedSize);
        dao.close();
        cleanUp(tmp);
    }

    @Test
    void compareBlockSizes() throws IOException {
        Path compressedTmp4KB = Files.createTempDirectory("compressedDao4KB");
        Path compressedTmp2KB = Files.createTempDirectory("compressedDao2KB");
        long flushThreshold = 1 << 20; // 1 MB

        Dao<String, Entry<String>> compressedDao2KB = factory.createStringDao(new Config(
                compressedTmp4KB,
                flushThreshold,
                new Config.CompressionConfig(true, Config.CompressionConfig.Compressor.LZ4, BLOCK_2KB)
        ));
        Dao<String, Entry<String>> compressedDao4KB = factory.createStringDao(new Config(
                compressedTmp2KB,
                flushThreshold,
                new Config.CompressionConfig(true, Config.CompressionConfig.Compressor.LZ4, BLOCK_4KB)
        ));
        List<Entry<String>> entries = entries(1000);
        entries.forEach(stringEntry -> {
            compressedDao2KB.upsert(stringEntry);
            compressedDao4KB.upsert(stringEntry);
        });
        // finish all bg processes
        compressedDao2KB.close();
        compressedDao4KB.close();

        long compressedSize2KB = sizePersistentData(compressedDao2KB);
        long compressedSize4KB = sizePersistentData(compressedDao4KB);
        assertTrue(compressedSize4KB < compressedSize2KB);
        cleanUp(compressedTmp2KB);
        cleanUp(compressedTmp4KB);
    }

    private static void cleanUp(Path tmp) throws IOException {
        if (!Files.exists(tmp)) {
            return;
        }
        Files.walkFileTree(tmp, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}

