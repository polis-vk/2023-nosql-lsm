package ru.vk.itmo.test.ryabovvadim.utils;

import ru.vk.itmo.test.ryabovvadim.iterators.FutureIterator;
import ru.vk.itmo.test.ryabovvadim.iterators.LazyIterator;

import java.util.NoSuchElementException;

public final class IteratorUtils {

    public static <T> FutureIterator<T> emptyFutureIterator() {
        return new LazyIterator<>(
                () -> {throw new NoSuchElementException();},
                () -> false
        );
    }

    private IteratorUtils () {
    }
}
