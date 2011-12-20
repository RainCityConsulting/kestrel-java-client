package com.rcc.kestrel;

import com.rcc.kestrel.async.*;

public interface AsynchronousClient extends Client {
    public boolean setAndForget(String queueName, long expiration, byte[] data);

    public boolean set(String queueName, long expiration, byte[] data, SetResponseHandler handler);

    public boolean get(
            String queueName, long timeoutMs, boolean reliable, GetResponseHandler handler);

    public boolean peek(String queueName, long timeoutMs, GetResponseHandler handler);

    public boolean deleteQueue(String queueName, DeleteResponseHandler handler);
    public boolean stats(StatsResponseHandler handler);
}
