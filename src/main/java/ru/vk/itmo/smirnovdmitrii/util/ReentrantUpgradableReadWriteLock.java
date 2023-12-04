package ru.vk.itmo.smirnovdmitrii.util;

import ru.vk.itmo.smirnovdmitrii.util.exceptions.LockError;

import java.util.concurrent.atomic.AtomicInteger;

public class ReentrantUpgradableReadWriteLock implements UpgradableReadWriteLock {
    private Thread owner = null;
    //                    16 bits             16 bits
    // 32 bits: |---read state part---|---write state part--|
    private final AtomicInteger state = new AtomicInteger(0);
    private final ThreadLocal<Integer> threadReads;
    private final ThreadLocal<Integer> threadWrites;
    private static final int MASK_SHIFT = 16;
    private static final int READ_UNIT = 1 << MASK_SHIFT;
    private static final int MAX_COUNT = (1 << MASK_SHIFT) - 1;
    private static final int WRITE_MASK = (1 << MASK_SHIFT) - 1;

    public ReentrantUpgradableReadWriteLock() {
        threadReads = ThreadLocal.withInitial(() -> 0);
        threadWrites = ThreadLocal.withInitial(() -> 0);
    }

    private int getReadCount(final int state) {
        return state >>> MASK_SHIFT;
    }

    private int getWriteCount(final int state) {
        return state & WRITE_MASK;
    }

    private int getThreadReads() {
        return threadReads.get();
    }

    private void incrementThreadReads() {
        threadReads.set(threadReads.get() + 1);
    }

    private void incrementThreadWrites() {
        threadWrites.set(threadWrites.get() + 1);
    }

    private void decrementThreadLocal(final ThreadLocal<Integer> threadLocal) {
        final int count = threadLocal.get();
        if (count == 0) {
            throw new IllegalMonitorStateException();
        }
        threadLocal.set(count - 1);
    }

    private void decrementThreadReads() {
        decrementThreadLocal(threadReads);
    }

    private void decrementThreadWrites() {
        decrementThreadLocal(threadWrites);
    }

    @Override
    public boolean tryWriteLock() {
        final Thread current = Thread.currentThread();
        final int s = state.get();
        if (s != 0) {
            final int w = getWriteCount(s);
            final int r = getReadCount(s);
            if ((w == 0 && r != getThreadReads()) || (w != 0 && owner != current)) {
                return false;
            }
            if (w >= MAX_COUNT) {
                throw new LockError("To many acquires");
            }
        }
        if (!state.compareAndSet(s, s + 1)) {
            return false;
        }
        owner = current;
        incrementThreadWrites();
        return true;
    }

    @Override
    public boolean tryReadLock() {
        Thread current = Thread.currentThread();
        while (true) {
            int c = state.get();
            if (getWriteCount(c) != 0 && owner != current) {
                return false;
            }
            int r = getReadCount(c);
            if (r == MAX_COUNT) {
                throw new LockError("Maximum lock count exceeded");
            }
            if (state.compareAndSet(c, c + READ_UNIT)) {
                incrementThreadReads();
                return true;
            }
        }
    }

    @Override
    public void readUnlock() {
        decrementThreadReads();
        while (true) {
            final int s = state.get();
            if (state.compareAndSet(s, s - READ_UNIT)) {
                return;
            }
        }
    }

    @Override
    public void writeUnlock() {
        decrementThreadWrites();
        state.decrementAndGet();
    }
}
