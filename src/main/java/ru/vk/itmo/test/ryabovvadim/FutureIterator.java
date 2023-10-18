package ru.vk.itmo.test.ryabovvadim;

import java.util.Iterator;

public interface FutureIterator<T> extends Iterator<T> {
    T showNext();
}
