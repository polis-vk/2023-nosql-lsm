package ru.vk.itmo.solonetsarseniy.transformer;

public interface Transformer<T, S> {
    T toTarget(S source);

    S toSource(T target);
}
