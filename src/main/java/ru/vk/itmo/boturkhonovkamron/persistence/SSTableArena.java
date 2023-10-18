package ru.vk.itmo.boturkhonovkamron.persistence;

import java.lang.foreign.Arena;

public record SSTableArena(Arena indexArena, Arena tableArena) {

    public boolean isAlive() {
        return indexArena.scope().isAlive() && tableArena.scope().isAlive();
    }

    public void close() {
        indexArena.close();
        tableArena.close();
    }
}
