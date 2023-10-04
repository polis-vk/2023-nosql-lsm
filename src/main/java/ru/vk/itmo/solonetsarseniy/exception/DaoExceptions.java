package ru.vk.itmo.solonetsarseniy.exception;

public enum DaoExceptions {
    NULL_KEY_PUT("Got null as Map key. Please do not try to put Entries with null keys."),
    NULL_KEY_GET("Got null as Map key. Please do not try to get Entries with null keys. "
        + "There is no such thing here!");

    private final String errorString;

    DaoExceptions(String errorString) {
        this.errorString = errorString;
    }

    public String getErrorString() {
        return errorString;
    }
}
