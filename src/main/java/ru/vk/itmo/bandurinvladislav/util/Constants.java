package ru.vk.itmo.bandurinvladislav.util;

public class Constants {
    private Constants() {}

    public static long INDEX_ROW_SIZE = 28;
    public static long INDEX_ROW_KEY_LENGTH_POSITION = Integer.BYTES;
    public static long INDEX_ROW_VALUE_LENGTH_POSITION = Integer.BYTES + Long.BYTES;
    public static long INDEX_ROW_OFFSET_POSITION = Integer.BYTES + 2 * Long.BYTES;

}
