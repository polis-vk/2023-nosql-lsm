package ru.vk.itmo.belonogovnikolay.exceptions;

/**
 * An exception is thrown when attempting to use a utility class without specifying a snapshot file directory.
 *
 * @author Belonogov Nikolay
 */
public class PathNotFoundException extends RuntimeException {

    public PathNotFoundException(String reason) {
        super(reason);
    }
}
