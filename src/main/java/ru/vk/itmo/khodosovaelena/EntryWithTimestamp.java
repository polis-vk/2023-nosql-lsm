package ru.vk.itmo.khodosovaelena;

public interface EntryWithTimestamp<D> {
    D key();

    D value();

    long timestamp();
}
