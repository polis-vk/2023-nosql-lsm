package ru.vk.itmo.test.ryabovvadim.memtable;

import ru.vk.itmo.Entry;

import java.util.concurrent.locks.ReentrantLock;

public class ChangeableEntryWithLock<T> implements Entry<T> {
    private final ReentrantLock lock = new ReentrantLock();
    private final T key;
    private T value;

    public ChangeableEntryWithLock(Entry<T> entry) {
        this.key = entry.key();
        this.value = entry.value();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    @Override
    public T key() {
        return key;
    }

    @Override
    public T value() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}
