package ru.vk.itmo.ershovvadim.hw3;

import java.util.Iterator;
import java.util.NoSuchElementException;

public interface PeekIterator<E> extends Iterator<E> {

    E peek() throws NoSuchElementException;

    int getPriority();
}
