package ru.vk.itmo.viktorkorotkikh;

public class SSTableReadException extends RuntimeException {
    public SSTableReadException(Throwable cause) {
        super(cause);
    }
}
