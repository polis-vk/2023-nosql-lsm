package ru.vk.itmo.prokopyevnikita;

import ru.vk.itmo.Config;

class State {
    final Config config;
    final Memory memory;
    final Memory flushing;
    final boolean closed;
    Storage storage;

    // State with opened storage
    State(Config config, Memory memory, Memory flushing, Storage storage) {
        this.config = config;
        this.memory = memory;
        this.flushing = flushing;
        this.storage = storage;
        this.closed = false;
    }

    // State with closed storage
    State(Config config, Storage storage) {
        this.config = config;
        this.memory = Memory.NOT_PRESENTED;
        this.flushing = Memory.NOT_PRESENTED;
        this.storage = storage;
        this.closed = true;
    }

    // initial State
    static State initState(Config config, Storage storage) {
        return new State(
                config,
                new Memory(config.flushThresholdBytes()),
                Memory.NOT_PRESENTED,
                storage
        );
    }

    public State prepareForFlush() {
        isClosedCheck();
        if (isFlushing()) {
            throw new IllegalStateException("Already flushing");
        }
        return new State(
                config,
                new Memory(config.flushThresholdBytes()),
                memory,
                storage
        );
    }

    public State afterFlush(Storage storage) {
        isClosedCheck();
        if (!isFlushing()) {
            throw new IllegalStateException("Wasn't flushing");
        }
        return new State(
                config,
                memory,
                Memory.NOT_PRESENTED,
                storage
        );
    }

    public State afterCompaction(Storage storage) {
        isClosedCheck();
        return new State(
                config,
                memory,
                flushing,
                storage
        );
    }

    public State afterClose() {
        isClosedCheck();
        if (!storage.isClosed()) {
            throw new IllegalStateException("Storage should be closed early");
        }
        return new State(config, storage);
    }

    public void isClosedCheck() {
        if (closed) {
            throw new IllegalStateException("Already closed");
        }
    }

    public boolean isFlushing() {
        return this.flushing != Memory.NOT_PRESENTED;
    }
}
