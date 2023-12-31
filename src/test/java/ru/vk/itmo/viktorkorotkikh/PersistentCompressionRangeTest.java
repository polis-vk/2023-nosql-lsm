package ru.vk.itmo.viktorkorotkikh;


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.vk.itmo.BaseTest;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.DaoFactory;
import ru.vk.itmo.test.viktorkorotkikh.FactoryImpl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.foreign.MemorySegment;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("JUnitMalformedDeclaration")
public class PersistentCompressionRangeTest extends BaseTest {
    private static final int BLOCK_4KB = 4 * 1024;

    private static final long flushThreshold = 1 << 20; // 1 MB
    private static final DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> factory = new FactoryImpl();

    public static final int[] NOTHING = new int[0];
    private static final int[] DATASET = new int[]{7, 97, 101};

    @ParameterizedTest(name = "{0}")
    @MethodSource("getDao")
    @Retention(RetentionPolicy.RUNTIME)
    private @interface CompressionTest {
    }

    private static final List<Path> tmpsToCleanUp = new ArrayList<>();

    public static Stream<Arguments> getDao() {
        try {
            Path lz4Tmp = Files.createTempDirectory("lz4dao");
            tmpsToCleanUp.add(lz4Tmp);
            Dao<String, Entry<String>> lz4Dao = factory.createStringDao(new Config(
                    lz4Tmp,
                    flushThreshold,
                    new Config.CompressionConfig(true, Config.CompressionConfig.Compressor.LZ4, BLOCK_4KB)
            ));

            Path zstdTmp = Files.createTempDirectory("zstddao");
            tmpsToCleanUp.add(zstdTmp);
            Dao<String, Entry<String>> zstdDao = factory.createStringDao(new Config(
                    zstdTmp,
                    flushThreshold,
                    new Config.CompressionConfig(true, Config.CompressionConfig.Compressor.LZ4, BLOCK_4KB)
            ));
            return Stream.of(Arguments.of(lz4Dao), Arguments.of(zstdDao));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @AfterAll
    static void cleanUpTmp() throws IOException {
        for (Path path : tmpsToCleanUp) {
            cleanUp(path);
        }
        tmpsToCleanUp.clear();
    }

    private void sliceAndDice(Dao<String, Entry<String>> dao) {
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

    @CompressionTest
    void justMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(101));
        dao.upsert(entryAt(97));
        dao.upsert(entryAt(7));

        sliceAndDice(dao);

        dao.close();
    }

    @CompressionTest
    void justDisk(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(97));
        dao.upsert(entryAt(7));
        dao.upsert(entryAt(101));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        sliceAndDice(dao);

        dao.close();
    }

    @CompressionTest
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

    @CompressionTest
    void mixedMemoryDisk(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(7));
        dao.upsert(entryAt(97));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entryAt(101));

        sliceAndDice(dao);
    }

    @CompressionTest
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

    @CompressionTest
    void replaceWithMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(7));
        dao.upsert(entry(keyAt(97), "old97"));
        dao.upsert(entryAt(101));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entryAt(97));

        sliceAndDice(dao);
    }

    @CompressionTest
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

    @CompressionTest
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

    @CompressionTest
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

    @CompressionTest
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

    @CompressionTest
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

    @CompressionTest
    void memoryCemetery(Dao<String, Entry<String>> dao) throws IOException {
        final int entries = 100_000;

        for (int entry = 0; entry < entries; entry++) {
            dao.upsert(entry(keyAt(entry), null));
        }

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        assertFalse(dao.all().hasNext());
    }

    @CompressionTest
    void diskCemetery(Dao<String, Entry<String>> dao) throws Exception {
        final int entries = 100_000;

        for (int entry = 0; entry < entries; entry++) {
            dao.upsert(entry(keyAt(entry), null));

            // Back off after 1K upserts to be able to flush
            if (entry % 1000 == 0) {
                Thread.sleep(1);
            }
        }

        assertFalse(dao.all().hasNext());
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
