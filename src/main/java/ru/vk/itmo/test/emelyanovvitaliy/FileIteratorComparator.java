package ru.vk.itmo.test.emelyanovvitaliy;

import java.util.Comparator;

public class FileIteratorComparator implements Comparator<FileIterator> {
    @Override
    public int compare(FileIterator o1, FileIterator o2) {
        if (o1.getTimestamp() == o2.getTimestamp()) {
            return Long.compare(o1.getRuntimeTimestamp(), o2.getRuntimeTimestamp());
        }
        return Long.compare(o1.getTimestamp(), o2.getTimestamp());
    }
}
