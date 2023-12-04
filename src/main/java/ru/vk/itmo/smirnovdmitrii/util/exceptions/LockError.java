package ru.vk.itmo.smirnovdmitrii.util.exceptions;

public class LockError extends Error {
    public LockError(final String message) {
        super(message);
    }
}
