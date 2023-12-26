package ru.vk.itmo.prokopyevnikita;

public class StorageClosedException extends RuntimeException {
    public StorageClosedException(Throwable cause) {
        super(cause);
    }
}
