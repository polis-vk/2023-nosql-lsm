package ru.vk.itmo.abramovilya.table;

public interface Table {
    TableEntry currentEntry();

    TableEntry nextEntry();
}
