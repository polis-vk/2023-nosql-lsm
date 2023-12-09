package ru.vk.itmo;

import java.nio.file.Path;

public record Config(
        Path basePath,
        long flushThresholdBytes,
        CompressionConfig compressionConfig
) {
    public record CompressionConfig(
            boolean enabled,
            Compressor compressor
    ) {
        enum Compressor {
            LZ4;
        }
    }

    public static CompressionConfig disableCompression() {
        return new CompressionConfig(false, null);
    }
}
