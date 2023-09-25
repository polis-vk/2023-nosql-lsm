package ru.vk.itmo.solonetsarseniy;

public enum DaoException {
    NULL_KEY_PUT("Got null as Map key. Please do not try to put Entries with null keys."),
    NULL_KEY_GET("Got null as Map key. Please do not try to get Entries with null keys. " +
         "There there is no such thing here!");

    private final String errorString;

    DaoException(String errorString) {
        this.errorString = errorString;
    }

    public void throwException() {
        throw new RuntimeException(errorString);
    }
}
