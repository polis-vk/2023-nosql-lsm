package ru.vk.itmo.chebotinalexandr;

/** Binary search in SSTable result information.
 */
public record FindResult(boolean found, long index) { }
