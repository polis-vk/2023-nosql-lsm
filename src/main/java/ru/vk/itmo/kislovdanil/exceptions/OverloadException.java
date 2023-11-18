package ru.vk.itmo.kislovdanil.exceptions;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

public class OverloadException extends DBException {
    public final Entry<MemorySegment> entry;

    public OverloadException(Entry<MemorySegment> entry) {
        super();
        this.entry = entry;
    }
}
