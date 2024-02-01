package ru.vk.itmo.reference;

public record SampledIndex(
        long keyAtTheStartOffset,
        long keyAtTheStartLength,
        long keyAtTheEndOffset,
        long keyAtTheEndLength,
        long offset
) {
    final static int SAMPLED_INDEX_STEP = 16;
}