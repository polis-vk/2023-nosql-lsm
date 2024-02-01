package ru.vk.itmo.kovalchukvladislav.model;

public class MemoryOverflowException extends RuntimeException {
    public MemoryOverflowException(String message) {
        super(message);
    }
}
