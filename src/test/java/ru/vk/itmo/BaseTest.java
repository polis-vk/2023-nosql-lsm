package ru.vk.itmo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.opentest4j.AssertionFailedError;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BaseTest {

    private final CopyOnWriteArrayList<ExecutorService> executors = new CopyOnWriteArrayList<>();

    public void assertEmpty(Iterator<?> iterator) {
        checkInterrupted();
        Assertions.assertIterableEquals(Collections.emptyList(), list(iterator));
    }

    public void assertSame(Entry<String> entry, Entry<String> expected) {
        checkInterrupted();
        Assertions.assertEquals(expected, entry);
    }

    public void assertNull(Entry<String> entry) {
        checkInterrupted();
        Assertions.assertNull(entry);
    }

    public void assertSame(Iterator<? extends Entry<String>> iterator, Entry<?>... expected) {
        assertSame(iterator, Arrays.asList(expected));
    }

    public void assertSame(Iterator<? extends Entry<String>> iterator, int... expected) {
        assertSame(iterator, IntStream.of(expected).mapToObj(this::entryAt).collect(Collectors.toList()));
    }

    public void assertSame(Iterator<? extends Entry<String>> iterator, List<? extends Entry<?>> expected) {
        int index = 0;
        for (Entry<?> entry : expected) {
            checkInterrupted();
            if (!iterator.hasNext()) {
                throw new AssertionFailedError("No more entries in iterator: " + index + " from " + expected.size() + " entries iterated");
            }
            int finalIndex = index;
            Assertions.assertEquals(entry, iterator.next(), () -> "wrong entry at index " + finalIndex + " from " + expected.size());
            index++;
        }
        if (iterator.hasNext()) {
            throw new AssertionFailedError("Unexpected entry at index " + index + " from " + expected.size() + " elements: " + iterator.next());
        }
    }

    public void assertContains(Iterator<? extends Entry<String>> iterator, Entry<String> entry) {
        int count = 0;
        while (iterator.hasNext()) {
            checkInterrupted();
            if (iterator.next().equals(entry)) {
                return;
            }
            count++;
        }
        throw new AssertionFailedError(entry + " not found in iterator with elements count " + count);
    }

    public void assertValueAt(Dao<String, Entry<String>> dao, int index) throws IOException {
        assertSame(dao.get(keyAt(index)), entryAt(index));
    }

    public void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Entry<String> entry(String key, String value) {
        checkInterrupted();
        return new BaseEntry<>(key, value);
    }

    public List<Entry<String>> entries(int count) {
        return entries("k", "v", count);
    }

    public List<Entry<String>> bigValues(int count, int valueSize) {
        char[] data = new char[valueSize / 2];
        Arrays.fill(data, 'V');
        return entries("k", new String(data), count);
    }

    public List<Entry<String>> entries(String keyPrefix, String valuePrefix, int count) {
        return new AbstractList<>() {
            @Override
            public Entry<String> get(int index) {
                checkInterrupted();
                if (index >= count || index < 0) {
                    throw new IndexOutOfBoundsException("Index is " + index + ", size is " + count);
                }
                String paddedIdx = String.format("%010d", index);
                return new BaseEntry<>(keyPrefix + paddedIdx, valuePrefix + paddedIdx);
            }

            @Override
            public int size() {
                return count;
            }
        };
    }

    public Entry<String> entryAt(int index) {
        return new BaseEntry<>(keyAt(index), valueAt(index));
    }

    public String keyAt(int index) {
        return keyAt("k", index);
    }

    public String valueAt(int index) {
        return valueAt("v", index);
    }

    public String keyAt(String prefix, int index) {
        String paddedIdx = String.format("%010d", index);
        return prefix + paddedIdx;
    }

    public String valueAt(String prefix, int index) {
        String paddedIdx = String.format("%010d", index);
        return prefix + paddedIdx;
    }

    public <T> List<T> list(Iterator<T> iterator) {
        List<T> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);
        return result;
    }

    public AutoCloseable runInParallel(int tasksCount, ParallelTask runnable) {
        return runInParallel(tasksCount, tasksCount, runnable);
    }

    public AutoCloseable runInParallel(int threadCount, int tasksCount, ParallelTask runnable, Runnable longTask) {
        ExecutorService service = Executors.newSingleThreadExecutor();
        executors.add(service);
        Future<?> submit = service.submit(longTask);
        return () -> {
            runInParallel(threadCount, tasksCount, runnable).close();
            submit.get();
        };
    }

    public AutoCloseable runInParallel(int threadCount, int tasksCount, ParallelTask runnable) {
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        executors.add(service);
        try {
            AtomicInteger index = new AtomicInteger();
            List<Future<Void>> futures = service.invokeAll(Collections.nCopies(threadCount, () -> {
                while (!Thread.interrupted()) {
                    int i = index.getAndIncrement();
                    if (i >= tasksCount) {
                        return null;
                    }
                    runnable.run(i);
                }
                throw new InterruptedException("Execution is interrupted");
            }));
            return () -> {
                for (Future<Void> future : futures) {
                    future.get();
                }
            };
        } catch (InterruptedException | OutOfMemoryError e) {
            throw new RuntimeException(e);
        }
    }

    public interface ParallelTask {
        void run(int taskIndex) throws Exception;
    }

    public void checkInterrupted() {
        if (Thread.interrupted()) {
            throw new RuntimeException(new InterruptedException());
        }
    }

    @AfterEach
    void shutdownExecutors() {
        for (ExecutorService executor : executors) {
            executor.shutdownNow();
        }
    }

    public void cleanUpPersistentData(Dao<String, Entry<String>> dao) throws IOException {
        Config config = DaoFactory.Factory.extractConfig(dao);
        cleanUpDir(config);
    }

    public void cleanUpDir(Config config) throws IOException {
        if (!Files.exists(config.basePath())) {
            return;
        }
        Files.walkFileTree(config.basePath(), new SimpleFileVisitor<>() {
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

    public long sizePersistentData(Dao<String, Entry<String>> dao) throws IOException {
        Config config = DaoFactory.Factory.extractConfig(dao);
        return sizePersistentData(config);
    }

    public long sizePersistentData(Config config) throws IOException {
        long[] result = new long[]{0};
        Files.walkFileTree(config.basePath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                result[0] += Files.size(file);
                return FileVisitResult.CONTINUE;
            }
        });
        return result[0];
    }

}
