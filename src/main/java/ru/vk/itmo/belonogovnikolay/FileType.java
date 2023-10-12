package ru.vk.itmo.belonogovnikolay;

/**
 * The class represents an enumeration of two file types. `data-file` stores data that is written when reconnecting,
 * disconnecting, etc. `offset-file` contains data about data offsets in `data-file`.
 * @author Belonogov Nikolay
 */

public enum FileType {

    DATA("data-file"),

    OFFSET("offset-file");

    private final String fileName;

    FileType(String name) {
        this.fileName = name;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
