package ru.vk.itmo.test.smirnovdmitrii;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.InMemoryDao;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Iterator;

public class TestMain {

    private static final DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> FACTORY = new MemorySegmentFactory();
    private static final Path DEFAULT_PATH = Path.of("TEST_LSM");
    private static final Config DEFAULT_CONFIG = new Config(DEFAULT_PATH);

    public static void main(String[] args) {
        new LSMTest().test();
        clear();
    }

    public static void clear() {
        try {
            Files.walkFileTree(DEFAULT_PATH, new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void assertEquals(Object a, Object b) {
        if (a == null && b == null) {
            return;
        }
        if (a == null || b == null) {
            throw new AssertionError();
        }
        if (a.equals(b)) {
            return;
        }
        throw new AssertionError();
    }

    public static Entry<String> fromSegments(final Entry<MemorySegment> entry) {
        return new BaseEntry<>(FACTORY.toString(entry.key()), FACTORY.toString(entry.value()));
    }

    public static Entry<MemorySegment> toSegments(final Entry<String> entry) {
        return new BaseEntry<>(FACTORY.fromString(entry.key()), FACTORY.fromString(entry.value()));
    }

    private static final class TestDao implements Dao<String, Entry<String>> {

        private final DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> factory;
        private final Config config;
        private InMemoryDao inMemoryDao;

        public TestDao(final DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> factory, final Config config) {
            this.factory = factory;
            this.config = config;
            inMemoryDao = new InMemoryDao(config);
        }

        @Override
        public Iterator<Entry<String>> get(final String from, final String to) {
            final Iterator<Entry<MemorySegment>> iterator =
                    inMemoryDao.get(factory.fromString(from), factory.fromString(to));
            return new Iterator<Entry<String>>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Entry<String> next() {
                    return fromSegments(iterator.next());
                }
            };
        }

        @Override
        public Entry<String> get(final String key) {
            return fromSegments(inMemoryDao.get(factory.fromString(key)));
        }

        @Override
        public void upsert(final Entry<String> entry) {
            inMemoryDao.upsert(toSegments(entry));
        }

        @Override
        public void close() throws IOException {
            inMemoryDao.close();
        }

        public void reopen() throws IOException {
            inMemoryDao.close();
            inMemoryDao = new InMemoryDao(config);
        }
    }

    private static Entry<String> entry(final String a, final String b) {
        return new BaseEntry<>(a, b);
    }

    private static final class LSMTest {
        public void test() {
            test(this::testSimple);
            test(this::testRepeatedRead);
            test(this::testSeveralKeys);
            test(this::testPerformance);
            test(this::testRepeatedReadSequence);
        }

        interface CheckedConsumer<T> {
            void accept(T t) throws IOException;
        }

        private void test(CheckedConsumer<TestDao> consumer) {
            try (final TestDao dao = new TestDao(FACTORY, DEFAULT_CONFIG)) {
                consumer.accept(dao);
            } catch (final Throwable e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }

        public void testSimple(final TestDao dao) throws IOException {
            dao.upsert(entry("a", "b"));
            dao.reopen();
            assertEquals("b", dao.get("a").value());
        }

        public void testRepeatedRead(final TestDao dao) throws IOException {
            dao.upsert(entry("a", "b"));
            dao.reopen();
            dao.upsert(entry("a", "c"));
            dao.reopen();
            assertEquals("c", dao.get("a").value());
        }

        public void testSeveralKeys(final TestDao dao) throws IOException {
            dao.upsert(entry("a", "b"));
            dao.reopen();
            dao.upsert(entry("c", "d"));
            dao.upsert((entry("e", "f")));
            dao.reopen();
            assertEquals("b", dao.get("a").value());
            assertEquals("d", dao.get("c").value());
            assertEquals("f", dao.get("e").value());
        }

        public void testPerformance(final TestDao dao) throws IOException {
            final Instant instant = Instant.now();
            final int count = 10_000;
            for (int i = 0; i < count; i++) {
                dao.upsert(entry("k" + i, "v" + i));
            }
            dao.reopen();
            for (int i = 0; i < count; i++) {
                assertEquals("v" + i, dao.get("k" + i).value());
            }
            System.out.println("performance time:" + (Instant.now().toEpochMilli() - instant.toEpochMilli()));
        }

        public void testRepeatedReadSequence(final TestDao dao) throws IOException {
            final Instant instant = Instant.now();
            final String k = "k" + 10000;
            final String v = "v" + 10000;
            dao.upsert(entry(k, v));
            dao.reopen();
            final int count = 10000;
            for (int i = 0; i < count; i++) {
                assertEquals(v, dao.get(k).value());
            }
            System.out.println("performance repeated read: " + (Instant.now().toEpochMilli() - instant.toEpochMilli()));
        }

    }

}
