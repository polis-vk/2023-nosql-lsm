package ru.vk.itmo.reference;

public record SampledIndex(
        long keyAtTheStartOffset,
        long keyAtTheStartLength,
        long offset
) {
    static final int SAMPLED_INDEX_STEP = 16;
}
