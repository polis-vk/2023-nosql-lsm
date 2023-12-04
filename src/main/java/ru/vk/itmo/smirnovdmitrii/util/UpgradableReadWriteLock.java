package ru.vk.itmo.smirnovdmitrii.util;

public interface UpgradableReadWriteLock {

    boolean tryWriteLock();

    boolean tryReadLock();

    void readUnlock();

    void writeUnlock();
}
