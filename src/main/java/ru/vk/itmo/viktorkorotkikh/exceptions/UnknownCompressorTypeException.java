package ru.vk.itmo.viktorkorotkikh.exceptions;

public class UnknownCompressorTypeException extends RuntimeException {
    public UnknownCompressorTypeException(byte compressorType) {
        super("Unknown compressor type - " + compressorType);
    }
}
