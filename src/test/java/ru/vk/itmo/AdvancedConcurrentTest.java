package ru.vk.itmo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Timeout;
import org.opentest4j.AssertionFailedError;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class AdvancedConcurrentTest extends BaseTest {

    void assertContainsAll(final Iterator<Entry<String>> iterator, final List<Entry<String>> entries) {
        int index = 0;
        final Comparator<String> stringComparator = Comparator.naturalOrder();
        while (iterator.hasNext() && index < entries.size()) {
            checkInterrupted();
            final Entry<String> now = iterator.next();
            final Entry<String> expected = entries.get(index);
            final int keyComp = stringComparator.compare(now.key(), expected.key());
            if (keyComp > 0) {
                break;
            } else if (keyComp < 0) {
                continue;
            }
            if (!now.equals(expected)) {
                break;
            }
            index++;
        }
        if (index != entries.size()) {
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }
            throw new AssertionFailedError("expected " + entries.size() + " elements, found " + index
                    + ", from " + entries.getFirst() + " to " + entries.getLast());
        }
    }
    void runParallel(
            final List<CheckedRunnable> runnables,
            final int timeout
    ) {
        final List<Throwable> excs = new ArrayList<>(Collections.nCopies(runnables.size(), null));
        try (ExecutorService service = Executors.newFixedThreadPool(runnables.size())) {
            IntStream.range(0, runnables.size()).forEach( i -> {
                final CheckedRunnable checkedRunnable = runnables.get(i);
                service.execute(() -> {
                    try {
                        checkedRunnable.run();
                    } catch (final InterruptedException | RuntimeException ignored) {
                        // do nothing
                    } catch (final AssertionFailedError | Exception e) {
                        System.out.println(LocalDateTime.now());
                        excs.set(i, e);
                    }
                });
            });
            sleep(timeout);
            service.shutdownNow();
        }
        Throwable e = null;
        for (final Throwable throwable: excs) {
            if (throwable == null) {
                continue;
            }

            if (e == null) {
                e = throwable;
            } else {
                e.addSuppressed(throwable);
            }
        }
        if (e != null) {
            Assertions.fail(e);
        }
    }

    @FunctionalInterface
    interface CheckedRunnable {
        void run() throws Exception;
    }

    @DaoTest(stage = 5)
    void testUpsertWhileFlush(final Dao<String, Entry<String>> dao) throws IOException {
        final int totalCount = 100;
        final List<Entry<String>> entries = entries(totalCount);
        final AtomicInteger counter = new AtomicInteger(0);
        final CheckedRunnable flusher = () -> {
            while (counter.get() < totalCount) {
                dao.flush();
                sleep(10);
            }
        };

        final CheckedRunnable upserter = () -> {
            for (final Entry<String> entry: entries) {
                dao.upsert(entry);
                sleep(5);
                counter.getAndIncrement();
            }
        };

        runParallel(List.of(upserter, flusher), 5_000);
        dao.close();
        final Dao<String, Entry<String>> reopened = DaoFactory.Factory.reopen(dao);
        assertSame(reopened.all(), entries);
    }

    @DaoTest(stage = 5)
    void testThreshold(final Dao<String, Entry<String>> dao) throws IOException {
        final String bigString = "//MY_STRING_IS_BIG//MY_STRING_IS_VERY_BIG//".repeat(1000);
        final List<Entry<String>> entries = new ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            final String bigStringCopy =
                    new String(bigString.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
            try {
                dao.upsert(entry(keyAt(i), bigStringCopy));
                entries.add(entry(keyAt(i), bigString));
            } catch (final Throwable ignored) {
                i--;
            }
        }
        dao.close();
        final Dao<String, Entry<String>> reopened = DaoFactory.Factory.reopen(dao);
        assertSame(reopened.all(), entries);
    }


    @DaoTest(stage = 5)
    @Timeout(30)
    void stressTest(final Dao<String, Entry<String>> dao) throws IOException {
        final int totalCount = 10_000;
        final List<Entry<String>> entries = entries(totalCount);
        for (final Entry<String> entry: entries) {
            dao.upsert(entry);
        }
        final List<List<Entry<String>>> permanentParts = new ArrayList<>();
        final int partCount = 10;

        for (int i = 1; i < partCount; i += 2) {
            permanentParts.add(entries.subList(totalCount / partCount * i, totalCount / partCount * (i + 1)));
        }
        final List<Entry<String>> temporalPart = new ArrayList<>();
        for (int i = 0; i < partCount; i += 2) {
            temporalPart.addAll(entries.subList(totalCount / partCount * i, totalCount / partCount * (i + 1)));
        }
        final CheckedRunnable flusher = () -> {
            while (!Thread.interrupted()) {
                dao.flush();
                sleep(15);
            }
        };

        final CheckedRunnable upserter = () -> {
            final Random random = ThreadLocalRandom.current();
            while (!Thread.interrupted()) {
                for (final Entry<String> entry: entries) {
                    if (Thread.interrupted()) {
                        return;
                    }
                    if (random.nextFloat(1) > 0.5) {
                        dao.upsert(entry);
                        sleep(5);
                    }
                }
            }
        };

        final CheckedRunnable remover = () -> {
            final Random random = ThreadLocalRandom.current();
            while(!Thread.interrupted()) {
                for (final Entry<String> entry: temporalPart) {
                    if (Thread.interrupted()) {
                        return;
                    }
                    if (random.nextFloat(1) > 0.5) {
                        dao.upsert(entry(entry.key(), null));
                        sleep(5);
                    }
                }
            }
        };

        final CheckedRunnable compacter = () -> {
            while (!Thread.interrupted()) {
                dao.compact();
                sleep(50);
            }
        };

        final CheckedRunnable checker = () -> {
            final Random random = ThreadLocalRandom.current();

            while (!Thread.interrupted()) {
                final int from = random.nextInt(permanentParts.size());
                final int to = random.nextInt(from, permanentParts.size());
                final List<Entry<String>> expected = new ArrayList<>();
                for (int i = from; i <= to; i++) {
                    expected.addAll(permanentParts.get(i));
                }
                final Entry<String> last = expected.removeLast();
                assertContainsAll(dao.get(expected.get(0).key(), last.key()), expected);
            }
        };
        final List<CheckedRunnable> workers = new ArrayList<>(Collections.nCopies(20, upserter));
        workers.addAll(Collections.nCopies(20, remover));
        workers.addAll(Collections.nCopies(3, flusher));
        workers.addAll(Collections.nCopies(3, compacter));
        workers.addAll(Collections.nCopies(10, checker));
        runParallel(workers, 10_000);
        dao.close();
    }
}
