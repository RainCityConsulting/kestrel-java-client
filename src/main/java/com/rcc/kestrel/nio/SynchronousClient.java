package com.rcc.kestrel.nio;

import com.rcc.kestrel.ClientException;
import com.rcc.kestrel.QueueStats;
import com.rcc.kestrel.async.*;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

public class SynchronousClient implements com.rcc.kestrel.SynchronousClient {
    private Client client;
    
    public SynchronousClient(String hostname, int port) {
        this(hostname, port, 1024, 8);
    }

    public SynchronousClient(
            String hostname, int port, int maxWorkQueueSize, int maxSocketPoolSize)
    {
        this.client = new Client(hostname, port, maxWorkQueueSize, maxSocketPoolSize);
    }

    @Override
    public void connect() {
        this.client.connect();
    }

    @Override
    public void disconnect() {
        this.client.disconnect();
    }

    public void init() throws IOException {
        this.client.init();
    }

    @Override
    public void set(String queueName, long expiration, byte[] data) throws IOException {
        try {
            final CountDownLatch latch = new CountDownLatch(1);

            final ClientException[] ex = new ClientException[1];

            client.set(queueName, expiration, data, new SetResponseHandler() {
                public void onSuccess() {
                    latch.countDown();
                }

                public void onError(String type, String message) {
                    ex[0] = new ClientException(String.format("[%s] %s", type, message));
                    latch.countDown();
                }
            });

            latch.await();

            if (ex[0] != null) { throw ex[0]; }
        } catch (InterruptedException e) {
            throw new ClientException(e);
        }
    }

    @Override
    public byte[] get(String queueName, long timeoutMs, boolean reliable) throws IOException {
        try {
            final CountDownLatch latch = new CountDownLatch(1);

            final ClientException[] ex = new ClientException[1];
            final Object[] result = new Object[1];

            client.get(queueName, timeoutMs, reliable, new GetResponseHandler() {
                public void onSuccess(byte[] data) {
                    result[0] = data;
                    latch.countDown();
                }

                public void onError(String type, String message) {
                    ex[0] = new ClientException(String.format("[%s] %s", type, message));
                    latch.countDown();
                }
            });

            latch.await();

            if (ex[0] != null) { throw ex[0]; }

            return (byte[]) result[0];
        } catch (InterruptedException e) {
            throw new ClientException(e);
        }
    }

    @Override
    public byte[] peek(String queueName, long timeoutMs) throws IOException {
        try {
            final CountDownLatch latch = new CountDownLatch(1);

            final ClientException[] ex = new ClientException[1];
            final Object[] result = new Object[1];

            this.client.peek(queueName, timeoutMs, new GetResponseHandler() {
                public void onSuccess(byte[] data) {
                    result[0] = data;
                    latch.countDown();
                }

                public void onError(String type, String message) {
                    ex[0] = new ClientException(String.format("[%s] %s", type, message));
                    latch.countDown();
                }
            });

            latch.await();

            if (ex[0] != null) { throw ex[0]; }

            return (byte[]) result[0];
        } catch (InterruptedException e) {
            throw new ClientException(e);
        }
    }

    @Override
    public Collection<QueueStats> stats() {
        try {
            final CountDownLatch latch = new CountDownLatch(1);

            final ClientException[] ex = new ClientException[1];
            final Object[] result = new Object[1];

            this.client.stats(new StatsResponseHandler() {
                public void onSuccess(Collection<QueueStats> stats) {
                    result[0] = stats;
                    latch.countDown();
                }

                public void onError(String type, String message) {
                    ex[0] = new ClientException(String.format("[%s] %s", type, message));
                    latch.countDown();
                }
            });

            latch.await();

            if (ex[0] != null) { throw ex[0]; }

            return (Collection<QueueStats>) result[0];
        } catch (InterruptedException e) {
            throw new ClientException(e);
        }
    }

    @Override
    public boolean deleteQueue(String queueName) {
        try {
            final CountDownLatch latch = new CountDownLatch(1);

            final ClientException[] ex = new ClientException[1];
            final boolean[] result = new boolean[] { false };

            this.client.deleteQueue(queueName, new DeleteResponseHandler() {
                public void onSuccess() {
                    result[0] = true;
                    latch.countDown();
                }

                public void onError(String type, String message) {
                    ex[0] = new ClientException(String.format("[%s] %s", type, message));
                    latch.countDown();
                }
            });

            latch.await();

            if (ex[0] != null) { throw ex[0]; }

            return result[0];
        } catch (InterruptedException e) {
            throw new ClientException(e);
        }
    }


    // Setters

    public void setClient(Client client) {
        this.client = client;
    }
}
