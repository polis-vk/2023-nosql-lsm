package ru.vk.itmo.test.ryabovvadim.memtable;

import ru.vk.itmo.Entry;

import java.util.concurrent.atomic.AtomicReference;

public class ChangeableEntryWithLock<T> implements Entry<T> {
    private final T key;
    private final AtomicReference<T> value;

    public ChangeableEntryWithLock(Entry<T> entry) {
        this.key = entry.key();
        this.value = new AtomicReference<>(entry.value());
    }

    @Override
    public T key() {
        return key;
    }

    @Override
    public T value() {
        return value.get();
    }

    public T getAndSet(T value) {
        return this.value.getAndSet(value);
    }
}
