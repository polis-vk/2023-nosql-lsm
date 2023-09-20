package ru.vk.itmo.test;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface DaoFactory {

    int stage() default 1;
    int week() default 1;

    interface Factory<D, E extends Entry<D>> {

        default Dao<D, E> createDao() {
            throw new UnsupportedOperationException("Need to override one of createDao methods");
        }

        default Dao<D, E> createDao(Config config) {
            return createDao();
        }

        String toString(D data);

        D fromString(String data);

        E fromBaseEntry(Entry<D> baseEntry);

        static Config extractConfig(Dao<String, Entry<String>> dao) {
            return ((TestDao<?,?>)dao).config;
        }

        static Dao<String, Entry<String>> reopen(Dao<String, Entry<String>> dao) {
            return ((TestDao<?,?>)dao).reopen();
        }

        default Dao<String, Entry<String>> createStringDao(Config config) {
            return new TestDao<>(this, config);
        }
    }

}
