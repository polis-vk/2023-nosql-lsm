package ru.vk.itmo.smirnovdmitrii.util.exceptions;

public class LockError extends RuntimeException {
    public LockError(final String message) {
        super(message);
    }
}
