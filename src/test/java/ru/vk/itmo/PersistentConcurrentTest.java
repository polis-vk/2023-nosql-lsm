package ru.vk.itmo;

import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.nio.channels.IllegalBlockingModeException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author andrey.timofeev
 */
public class PersistentConcurrentTest extends BaseTest {
    @DaoTest(stage = 4)
    void testConcurrentRW_2_500_2(Dao<String, Entry<String>> dao) throws Exception {
        int count = 2_500;
        List<Entry<String>> entries = entries("k", "v", count);
        runInParallel(100, count, value -> {
            dao.upsert(entries.get(value));
        }).close();
        dao.close();

        Dao<String, Entry<String>> dao2 = DaoFactory.Factory.reopen(dao);
        runInParallel(100, count, value -> {
            assertSame(dao2.get(entries.get(value).key()), entries.get(value));
        }).close();
    }

    @DaoTest(stage = 5)
    void testConcurrentRW_100_000_compact(Dao<String, Entry<String>> dao) throws Exception {
        int count = 100_000;

        List<Entry<String>> entries = entries("k", "v", count);
        long timeoutNanosWarmup = TimeUnit.MILLISECONDS.toNanos(1000);
        runInParallel(100, count, value -> {
            tryRun(timeoutNanosWarmup, () -> dao.upsert(entries.get(value)));
            tryRun(timeoutNanosWarmup, () -> dao.upsert(entry(keyAt(value), null)));
            tryRun(timeoutNanosWarmup, () -> dao.upsert(entries.get(value)));
        }, () -> {
            for (int i = 0; i < 100; i++) {
                try {
                    runAndMeasure(timeoutNanosWarmup, dao::compact);
                    runAndMeasure(timeoutNanosWarmup, dao::flush);

                    Thread.sleep(30);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).close();

        // 200ms should be enough considering GC
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(200);

        runInParallel(100, count, value -> {
            tryRun(timeoutNanos, () -> dao.upsert(entries.get(value)));
            tryRun(timeoutNanos, () -> dao.upsert(entry(keyAt(value), null)));
            tryRun(timeoutNanos, () -> dao.upsert(entries.get(value)));
        }, () -> {
            for (int i = 0; i < 100; i++) {
                try {
                    runAndMeasure(timeoutNanos, dao::compact);
                    runAndMeasure(timeoutNanos, dao::flush);

                    Thread.sleep(30);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).close();
        dao.close();

        Dao<String, Entry<String>> dao2 = DaoFactory.Factory.reopen(dao);
        runInParallel(
                100,
                count,
                value -> assertSame(dao2.get(entries.get(value).key()), entries.get(value))).close();
    }

    private static <E extends Exception> void runAndMeasure(
            long timeoutNanos,
            ErrorableTask<E> runnable) throws E {
        long start = System.nanoTime();
        runnable.run();
        long elapsedNanos = System.nanoTime() - start;

        // Check timeout
        if (elapsedNanos > timeoutNanos) {
            throw new IllegalBlockingModeException();
        }
    }

    private static void tryRun(
            long timeoutNanos,
            Runnable runnable) throws InterruptedException {
        long elapsedNanos;
        while (true) {
            try {
                long start = System.nanoTime();
                runnable.run();
                elapsedNanos = System.nanoTime() - start;
                break;
            } catch (Exception e) {
                //noinspection BusyWait
                Thread.sleep(100);
            }
        }

        // Check timeout
        if (elapsedNanos > timeoutNanos) {
            throw new IllegalBlockingModeException();
        }
    }
}
