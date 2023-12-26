package ru.vk.itmo.novichkovandrew.exceptions;

public class ReadFailureException extends RuntimeException {
    public ReadFailureException(String message) {
        super(message);
    }

    public ReadFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
