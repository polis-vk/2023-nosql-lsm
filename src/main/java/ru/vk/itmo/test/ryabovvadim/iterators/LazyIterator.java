package ru.vk.itmo.test.ryabovvadim.iterators;

import java.util.function.Supplier;

public class LazyIterator<T> implements FutureIterator<T> {
    private final Supplier<T> loadEntry;
    private final Supplier<Boolean> hasNextEntry;
    private T next;

    public LazyIterator(Supplier<T> getEntry, Supplier<Boolean> hasNextEntry) {
        this.loadEntry = getEntry;
        this.hasNextEntry = hasNextEntry;
    }

    @Override
    public boolean hasNext() {
        return next != null || hasNextEntry.get();
    }

    @Override
    public T next() {
        if (next != null) {
            T result = next;
            next = null;
            return result;
        }

        return loadEntry.get();
    }

    @Override
    public T showNext() {
        if (next == null) {
            next = next();
        }
        return next;
    }
}
