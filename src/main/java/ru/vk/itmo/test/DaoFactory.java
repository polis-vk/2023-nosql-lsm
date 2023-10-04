package ru.vk.itmo.test;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface DaoFactory {

    int stage() default 1;
    int week() default 1;

    interface Factory<Data, E extends Entry<Data>> {

        default Dao<Data, E> createDao() throws IOException {
            throw new UnsupportedOperationException("Need to override one of createDao methods");
        }

        default Dao<Data, E> createDao(Config config) throws IOException {
            return createDao();
        }

        String toString(Data data);

        Data fromString(String data);

        E fromBaseEntry(Entry<Data> baseEntry);

        static Config extractConfig(Dao<String, Entry<String>> dao) {
            return ((TestDao<?,?>)dao).config;
        }

        static Dao<String, Entry<String>> reopen(Dao<String, Entry<String>> dao) throws IOException {
            return ((TestDao<?,?>)dao).reopen();
        }

        default Dao<String, Entry<String>> createStringDao(Config config) throws IOException {
            return new TestDao<>(this, config);
        }
    }

}
