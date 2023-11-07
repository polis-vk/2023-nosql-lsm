package ru.vk.itmo.kislovdanil.exceptions;

public class DBException extends RuntimeException {
    public DBException(Exception e) {
        super(e);
    }
}
