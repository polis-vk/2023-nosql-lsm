package ru.vk.itmo.plyasovklimentii;

class MutableLong {
    private long value;

    public MutableLong(long value) {
        this.value = value;
    }

    public void increment() {
        this.value++;
    }

    public long getValue() {
        return this.value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public void incrementBy(long inc) {
        this.value += inc;
    }
}
