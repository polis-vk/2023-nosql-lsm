package ru.vk.itmo.smirnovdmitrii.transaction;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.util.ReentrantUpgradableReadWriteLock;
import ru.vk.itmo.smirnovdmitrii.util.UpgradableReadWriteLock;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LockTransaction<T, E extends Entry<T>> implements Transaction<T, E> {
    private static final AtomicLong transactionN = new AtomicLong();
    private final long revision = transactionN.getAndIncrement();
    private final HashMap<T, E> cache = new HashMap<>();
    private final HashMap<T, UpgradableReadWriteLock> localLocks = new LinkedHashMap<>();
    private final Dao<T, E> dao;
    private final TransactionGroup<T> group;

    public LockTransaction(
            final Dao<T, E> dao,
            final TransactionGroup<T> group
    ) {
        this.dao = dao;
        this.group = group;
    }

    @Override
    public E get(final T key) {
        Objects.requireNonNull(key);
        final E cached = cache.get(key);
        if (cached != null) {
            return cached;
        }
        final UpgradableReadWriteLock lock = group.sharedMap.computeIfAbsent(
                key, k -> new ReentrantUpgradableReadWriteLock()
        );
        if (!lock.tryReadLock()) {
            throw new ConcurrentModificationException(
                    "while getting " + key + " in transaction " + revision);
        }
        localLocks.put(key, lock);
        return dao.get(key);
    }

    @Override
    public void upsert(final E e) {
        Objects.requireNonNull(e);
        final T key = e.key();
        final E cached = cache.put(key, e);
        if (cached != null) {
            return;
        }
        final UpgradableReadWriteLock lock = group.sharedMap.computeIfAbsent(
                key, k -> new ReentrantUpgradableReadWriteLock()
        );
        if (!lock.tryWriteLock()) {
            throw new ConcurrentModificationException(
                    "while upserting " + e + " in transaction " + revision);
        }
        localLocks.put(key, lock);
    }

    @Override
    public void commit() {
        for (final Map.Entry<T, UpgradableReadWriteLock> entry: localLocks.entrySet()) {
            final UpgradableReadWriteLock lock = entry.getValue();
            final E value = cache.get(entry.getKey());
            if (value != null) {
                dao.upsert(value);
                lock.writeUnlock();
            } else {
                lock.readUnlock();
            }
        }
        cache.clear();
        localLocks.clear();
    }

    public static class TransactionGroup<T> {
        public final Map<T, UpgradableReadWriteLock> sharedMap = new ConcurrentHashMap<>();
    }

}
