package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

public record EntryWithTimestamp<Data>(Entry<Data> entry, long timestamp) {
}
