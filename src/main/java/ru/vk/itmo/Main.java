package ru.vk.itmo;

import ru.vk.itmo.test.ryabovvadim.InMemoryDao;
import ru.vk.itmo.test.ryabovvadim.InMemoryDaoFactory;

import java.nio.file.Path;

public class Main {
    public static void main(String[] args) {
        InMemoryDaoFactory factory = new InMemoryDaoFactory();
        InMemoryDao inMemoryDao = new InMemoryDao(new Config(Path.of(".")));
        inMemoryDao.upsert(new BaseEntry<>(
                factory.fromString("heh"),
                factory.fromString("nas")
        ));
        String a = factory.toString(inMemoryDao.get(factory.fromString("heh")).value());
        System.out.println(a);
    }
}
