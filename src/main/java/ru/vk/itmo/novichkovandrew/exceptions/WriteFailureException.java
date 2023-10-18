package ru.vk.itmo.novichkovandrew.exceptions;

public class WriteFailureException extends RuntimeException {
    public WriteFailureException(String message) {
        super(message);
    }

    public WriteFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
