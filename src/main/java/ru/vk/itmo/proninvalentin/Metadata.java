package ru.vk.itmo.proninvalentin;

public class Metadata {
    public static final long ENTRY_OFFSET_SIZE = Long.BYTES;
    public static final long IS_DELETED_SIZE = Byte.BYTES;
    public static final long CREATED_AT_SIZE = Long.BYTES;
    public static final long SIZE = ENTRY_OFFSET_SIZE + IS_DELETED_SIZE + CREATED_AT_SIZE;
    public final long entryOffset;
    public final boolean isDeleted;
    public final long createdAt;

    public Metadata(long entryOffset, boolean isDeleted, long createdAt) {
        this.entryOffset = entryOffset;
        this.isDeleted = isDeleted;
        this.createdAt = createdAt;
    }
}
