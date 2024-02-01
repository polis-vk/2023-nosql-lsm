package ru.vk.itmo.kovalchukvladislav.model;

import java.util.logging.Level;
import java.util.logging.Logger;

// Логгер, который я включаю локально, но выключаю перед пушем, чтобы он не засорял гитхаб.
public final class SimpleDaoLoggerUtility {
    private SimpleDaoLoggerUtility() {
    }

    public static Logger createLogger(Class<?> clazz) {
        Logger logger = Logger.getLogger(clazz.getSimpleName());
        logger.setLevel(Level.OFF);
        return logger;
    }
}
