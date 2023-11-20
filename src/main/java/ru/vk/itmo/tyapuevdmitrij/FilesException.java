package ru.vk.itmo.tyapuevdmitrij;

public class FilesException extends RuntimeException {
    public FilesException(String message, Throwable cause) {
        super(message, cause);
    }

    public FilesException(String message) {
        super(message);
    }
}
