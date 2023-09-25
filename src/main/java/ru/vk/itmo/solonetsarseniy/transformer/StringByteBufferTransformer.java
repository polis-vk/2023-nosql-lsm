package ru.vk.itmo.solonetsarseniy.transformer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringByteBufferTransformer implements Transformer<String, ByteBuffer> {
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private final Charset charset;

    public StringByteBufferTransformer(Charset charset) {
        this.charset = charset;
    }

    public StringByteBufferTransformer() {
        this.charset = DEFAULT_CHARSET;
    }

    @Override
    public String toTarget(ByteBuffer source) {
        byte[] byteArray = new byte[source.remaining()];
        source.get(byteArray);
        return new String(byteArray, charset);
    }

    @Override
    public ByteBuffer toSource(String target) {
        byte[] byteArr = target.getBytes(charset);
        return ByteBuffer.wrap(byteArr);
    }
}
