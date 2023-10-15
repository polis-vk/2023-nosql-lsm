package ru.vk.itmo.novichkovandrew;


import ru.vk.itmo.Entry;

/**
 * Inspired by cassandra db.
 */
public abstract class Cell<T> implements Entry<T> {
    public abstract long valueSize();

    public abstract boolean isTombstone();
}
