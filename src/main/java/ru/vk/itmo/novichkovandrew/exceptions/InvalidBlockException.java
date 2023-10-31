package ru.vk.itmo.novichkovandrew.exceptions;

public class InvalidBlockException extends RuntimeException {
    public InvalidBlockException(String message) {
        super(message);
    }

    public InvalidBlockException(String message, Throwable cause) {
        super(message, cause);
    }
}
