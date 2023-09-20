package ru.mail.polis;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.opentest4j.AssertionFailedError;

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
import java.util.function.IntConsumer;

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

    public void assertSame(Iterator<? extends Entry<String>> iterator, Entry<?>... expected) {
        assertSame(iterator, Arrays.asList(expected));
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
        while(iterator.hasNext()) {
            checkInterrupted();
            if (iterator.next().equals(entry)) {
                return;
            }
            count++;
        }
        throw new AssertionFailedError(entry + " not found in iterator with elements count " + count);
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

    public AutoCloseable runInParallel(int tasksCount, IntConsumer runnable) {
        return runInParallel(tasksCount, tasksCount, runnable);
    }

    public AutoCloseable runInParallel(int threadCount, int tasksCount, IntConsumer runnable) {
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
                    runnable.accept(i);
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

}
