package ru.vk.itmo.solonetsarseniy;

public interface Transformer<T, S> {
    T toTarget(S source);

    S toSource(T target);
}
