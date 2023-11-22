package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

import java.nio.file.Path;

public abstract class AbstractSSTable implements Comparable<AbstractSSTable> {
    protected final Path path;
    protected final long priority;

    protected AbstractSSTable(final Path path, final long priority) {
        this.path = path;
        this.priority = priority;
    }

    public Path path() {
        return path;
    }

    public long priority() {
        return priority;
    }

    @Override
    public int compareTo(final AbstractSSTable o) {
        return Long.compare(o.priority, this.priority);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(this.priority);
    }
}
