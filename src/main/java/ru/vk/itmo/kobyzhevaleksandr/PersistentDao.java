package ru.vk.itmo.kobyzhevaleksandr;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>>, Iterable<Entry<MemorySegment>> {

    private final MemorySegmentComparator memorySegmentComparator = new MemorySegmentComparator();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Config config;
    private State state;

    /*
    Filling ssTable with bytes from the memory segment with a structure:
    [key_size][key][value_size][value]...

    If value is null then value_size = -1
     */
    public PersistentDao(Config config) {
        this.config = config;
        this.state = new State(new ConcurrentSkipListMap<>(memorySegmentComparator), null, new Storage(config));
    }

    /*
    First, the state is fixed, and we get iterators for the memory table,
    for the flushing memory table, if there is one, and for the storage.
     */
    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        State currentState = state;
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        iterators.add(getMemoryIterator(from, to, currentState.memoryTable));
        if (currentState.flushingMemoryTable != null) {
            iterators.add(getMemoryIterator(from, to, currentState.flushingMemoryTable));
        }
        iterators.add(currentState.storage.iterator(from, to));
        return new SkipNullIterator(GlobalIterator.merge(iterators));
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = state.memoryTable.get(key);
        if (entry != null && entry.value() != null) {
            return entry;
        }

        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }

        Entry<MemorySegment> next = iterator.next();
        if (memorySegmentComparator.compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    /*
    First, the state is fixed and a check is made for multiple flushes.
    Next, the entry is placed in the memory table with the new size recorded in bytes and
    an automatic flush is performed if the threshold has been exceeded.
     */
    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Entry cannot be null.");
        }

        State currentState = state;
        if (currentState.memoryTableByteSize.get() >= config.flushThresholdBytes()
            && currentState.flushingMemoryTable != null) {
            throw new ApplicationException("Memory table is full, overload possible");
        }

        long valueSize = entry.value() == null ? 0 : entry.value().byteSize();
        lock.readLock().lock();
        try {
            currentState.memoryTable.put(entry.key(), entry);
            currentState.memoryTableByteSize.addAndGet(Long.BYTES + entry.key().byteSize() + Long.BYTES + valueSize);
        } finally {
            lock.readLock().unlock();
        }

        try {
            if (currentState.memoryTableByteSize.get() >= config.flushThresholdBytes()) {
                flush();
            }
        } catch (IOException e) {
            throw new ApplicationException("Can't auto flush", e);
        }
    }

    /*
    First, a check is made to ensure that the memory table is not empty and
    that the flushing memory table does not exist, otherwise the flush is not produced
    (errors are thrown to the client only when upserting, so when calling flush normally,
    the attempt is ignored when a flush is already running). Next, the state changes and the executor
    receives a task for the flush.

    The method marked as synchronized, which allows it to be executed in only one thread,
    blocking calls from other threads for a very small amount of time (less than a few milliseconds).
     */
    @Override
    public synchronized void flush() throws IOException {
        if (state.memoryTable.isEmpty() || state.flushingMemoryTable != null) {
            return;
        }

        lock.writeLock().lock();
        try {
            state = new State(new ConcurrentSkipListMap<>(memorySegmentComparator), state.memoryTable, state.storage);
        } finally {
            lock.writeLock().unlock();
        }
        executor.execute(this::tryToFlush);
    }

    @Override
    public synchronized void compact() {
        executor.execute(() -> state.storage.compact());
    }

    /*
    First, a check is made to see whether the dao was previously disabled or not
    (atomic boolean allows us to close the method only once when simultaneously calling a method
    from different threads). Next, the executor is shutting down and the contents of the memory table are flashed.
    At the end, the storage itself is closed.
     */
    @Override
    public void close() throws IOException {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (!state.memoryTable.isEmpty()) {
            state = new State(new ConcurrentSkipListMap<>(memorySegmentComparator), state.memoryTable, state.storage);
            tryToFlush();
        }
        state.storage.save();
        state = null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return all();
    }

    private Iterator<Entry<MemorySegment>> getMemoryIterator(
        MemorySegment from, MemorySegment to, NavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable
    ) {
        if (from == null && to == null) {
            return memoryTable.values().iterator();
        } else if (from == null) {
            return memoryTable.headMap(to).values().iterator();
        } else if (to == null) {
            return memoryTable.tailMap(from).values().iterator();
        }
        return memoryTable.subMap(from, to).values().iterator();
    }

    private void tryToFlush() {
        try {
            state.storage.flush(state.flushingMemoryTable.values());
            lock.writeLock().lock();
            try {
                state = new State(state.memoryTable, null, state.storage);
            } finally {
                lock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new ApplicationException("Can't flush memory table", e);
        }
    }

    private static class State {

        private final NavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable;
        private final NavigableMap<MemorySegment, Entry<MemorySegment>> flushingMemoryTable;
        private final AtomicLong memoryTableByteSize;
        private final Storage storage;

        private State(NavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable,
                      NavigableMap<MemorySegment, Entry<MemorySegment>> flushingMemoryTable,
                      Storage storage) {
            this.memoryTable = memoryTable;
            this.flushingMemoryTable = flushingMemoryTable;
            this.memoryTableByteSize = new AtomicLong();
            this.storage = storage;
        }
    }
}
