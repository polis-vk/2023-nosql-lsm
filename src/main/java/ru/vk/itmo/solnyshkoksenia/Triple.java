package ru.vk.itmo.solnyshkoksenia;

import ru.vk.itmo.Entry;

public record Triple<Data>(Data key, Data value, Data expiration) implements Entry<Data> {
    @Override
    public String toString() {
        return "{" + key + ":" + value + ":" + expiration + "}";
    }
}
