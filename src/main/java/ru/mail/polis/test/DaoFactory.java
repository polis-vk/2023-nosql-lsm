package ru.mail.polis.test;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

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

        default Dao<Data, E> createDao() {
            throw new UnsupportedOperationException("Need to override one of createDao methods");
        }

        default Dao<Data, E> createDao(Config config) {
            return createDao();
        }

        String toString(Data data);
        Data fromString(String data);
        E fromBaseEntry(Entry<Data> baseEntry);

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
