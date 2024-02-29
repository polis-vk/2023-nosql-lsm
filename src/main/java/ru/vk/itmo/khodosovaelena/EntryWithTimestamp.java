package ru.vk.itmo.khodosovaelena;

import ru.vk.itmo.Entry;

public interface EntryWithTimestamp<D> extends Entry<D> {
    long timestamp();
}
