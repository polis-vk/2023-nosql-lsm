package ru.vk.itmo.grunskiialexey.model;

/*
    I save in index.idx only positions in interval [a, b) of actual ss-tables.
    If you see 0, 0 => then you have no ss-tables;
    If you see 2, 5 => then you have "2", "3", "4" ss-tables
 */
public record ActualFilesInterval(long left, long right) {
}
