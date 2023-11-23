package ru.vk.itmo.test.ryabovvadim.memtable;

import ru.vk.itmo.Entry;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class ChangeableEntryWithLock<T> implements Entry<T> {
    private final T key;
    private AtomicReference<T> value;

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

    public void setValue(T value) {
        this.value.set(value);
    }
}
