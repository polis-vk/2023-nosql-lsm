package ru.vk.itmo.smirnovdmitrii.util;

import java.util.concurrent.locks.ReadWriteLock;

public interface UpgradableReadWriteLock {
    boolean tryWriteLock();
    boolean tryReadLock();

    void readUnlock();
    void writeUnlock();
}
