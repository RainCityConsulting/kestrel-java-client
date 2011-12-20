package com.rcc.kestrel;

import java.io.IOException;

public interface SynchronousClient extends Client {
    public void set(String queueName, long expiration, byte[] data) throws IOException;

    public byte[] get(String queueName, long timeoutMs, boolean reliable) throws IOException;

    public byte[] peek(String queueName, long timeoutMs) throws IOException;
}
