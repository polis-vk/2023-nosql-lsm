package ru.vk.itmo.boturkhonovkamron.util;

public class Offset {

    private long value = 0;

    public Offset() {
    }

    public Offset(final long initialValue) {
        this.value = initialValue;
    }

    public long get() {
        return this.value;
    }

    public long getAndAdd(final long delta) {
        final long tmp = this.value;
        this.value += delta;
        return tmp;
    }
}
