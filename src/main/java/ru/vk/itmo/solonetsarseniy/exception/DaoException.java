package ru.vk.itmo.solonetsarseniy.exception;

public class DaoException extends RuntimeException {

    public DaoException(String message) {
        super(message);
    }

    public static void throwException(DaoExceptions exception) {
        String errorMessage = exception.getErrorString();
        throw new DaoException(errorMessage);
    }
}
