package ru.vk.itmo;

import java.nio.file.Path;

public record Config(
        Path basePath,
        long flushThresholdBytes) {
}
