package ru.vk.itmo.solonetsarseniy.exception;

public class DaoException extends RuntimeException {

    public DaoException(String message) {
        super(message);
    }

    public DaoException(String message, Exception parent) {
        super(message, parent);
    }

    public static void throwException(DaoExceptions exception) {
        String errorMessage = exception.getErrorString();
        throw new DaoException(errorMessage);
    }

    public static void throwException(DaoExceptions exception, Exception parent) {
        String errorMessage = exception.getErrorString();
        throw new DaoException(errorMessage, parent);
    }
}
