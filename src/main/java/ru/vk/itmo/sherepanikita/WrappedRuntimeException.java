package ru.vk.itmo.sherepanikita;

public class WrappedRuntimeException extends RuntimeException {

    public WrappedRuntimeException(Throwable t) {
        super(t);
    }
}
