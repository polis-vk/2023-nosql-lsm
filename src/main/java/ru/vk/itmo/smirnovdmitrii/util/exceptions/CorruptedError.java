package ru.vk.itmo.smirnovdmitrii.util.exceptions;

public class CorruptedError extends Error {
    public CorruptedError(final String message) {
        super(message);
    }
}
