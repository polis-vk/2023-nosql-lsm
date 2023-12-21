package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.util.Iterator;

public interface DaoFileGet<D, E extends Entry<D>> {

    Iterator<E> get(D from, D to) throws IOException;

    E get(D key) throws IOException;
}
