package ru.vk.itmo.kobyzhevaleksandr;

public class ApplicationException extends RuntimeException {

    public ApplicationException(String message, Throwable cause) {
        super(message, cause);
    }
}
