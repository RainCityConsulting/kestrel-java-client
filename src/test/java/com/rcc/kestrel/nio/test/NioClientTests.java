package com.rcc.kestrel.nio.test;

import com.rcc.kestrel.AsynchronousClient;
import com.rcc.kestrel.QueueStats;
import com.rcc.kestrel.nio.Client;
import com.rcc.kestrel.async.*;

import static org.apache.commons.lang.RandomStringUtils.*;

import org.junit.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.*;

public class NioClientTests {
    private Client client;
    private String queueName;

    @Before
    public void setup() throws IOException {
        String hostname = System.getProperty("kestrel.hostname");
        int port = Integer.getInteger("kestrel.port").intValue();
        this.client = new Client(hostname, port, 200000, 512);
        client.init();

        this.queueName = "test_" + randomAlphabetic(32);

        // Ensure that the queue is empty
        //while (client.get(queueName, None).isDefined) {
            //println("Removed item off queue: " + queueName)
        //}
    }

    @After
    public void tearDown() throws IOException {
        final String qn = this.queueName;

        this.client.deleteQueue(qn, new DeleteResponseHandler() {
            public void onSuccess() { ; }

            public void onError(String type, String message) {
                System.err.println(String.format(
                        "Failed to  delete queue [%s] [%s] [%s]", qn, type, message));
            }
        });
        this.client.disconnect();
    }

    @Test
    public void testSet() throws Throwable {
        final CountDownLatch writeLatch = new CountDownLatch(1);
        final byte[] value = randomAlphanumeric(128).getBytes();

        final boolean[] passed = new boolean[] { false };

        this.client.set(this.queueName, 0L, value, new SetResponseHandler() {
            public void onSuccess() {
                try {
                    synchronized (passed) { passed[0] = true; }
                } finally {
                    writeLatch.countDown();
                }
            }

            public void onError(String type, String message) {
                System.out.println("Error on set :(");
            }
        });

        writeLatch.await(2, SECONDS);
        assertEquals(0, writeLatch.getCount());
        synchronized (passed) { assertTrue(passed[0]); }

        //System.out.println("Sleeping for 60 seconds");
        //Thread.sleep(60000);
        //System.out.println("Done sleeping");

        synchronized (passed) { passed[0] = false; }
        final CountDownLatch readLatch = new CountDownLatch(1);
        this.client.get(this.queueName, 0L, false, new GetResponseHandler() {
            public void onSuccess(byte[] data) {
                try {
                    synchronized (passed) { passed[0] = true; }
                } finally {
                    readLatch.countDown();
                }
            }

            public void onError(String type, String message) {
                System.out.println("Error on set :(");
            }
        });

        readLatch.await(2, SECONDS);
        assertEquals(0, readLatch.getCount());
        synchronized (passed) { assertTrue(passed[0]); }
    }

    @Test
    public void testExpiration() throws Throwable {
        final byte[] value = randomAlphanumeric(128).getBytes();

        final boolean[] passed = new boolean[] { false };

        final CountDownLatch writeLatch = new CountDownLatch(1);
        client.set(this.queueName, 1L, value, new SetResponseHandler() {
            public void onSuccess() {
                try {
                    synchronized (passed) { passed[0] = true; }
                } finally {
                    writeLatch.countDown();
                }
            }

            public void onError(String type, String message) {
                System.out.println("Error on set :(");
            }
        });

        writeLatch.await(2, SECONDS);

        synchronized (passed) { assertTrue(passed[0]); }

        Thread.sleep(2000);

        synchronized (passed) { passed[0] = false; }
        final CountDownLatch readLatch = new CountDownLatch(1);
        this.client.get(this.queueName, 0L, false, new GetResponseHandler() {
            public void onSuccess(byte[] data) {
                try {
                    if (data.length == 0) {
                        synchronized (passed) { passed[0] = true; }
                    }
                } finally {
                    readLatch.countDown();
                }
            }

            public void onError(String type, String message) { ; }
        });

        readLatch.await(2, SECONDS);
        synchronized (passed) { assertTrue(passed[0]); }
    }

    @Test
    public void testSetAndForget() throws Throwable {
        final byte[] value = randomAlphanumeric(128).getBytes();

        client.setAndForget(this.queueName, 1L, value);

        final boolean[] passed = new boolean[] { false };

        final CountDownLatch readLatch = new CountDownLatch(1);
        this.client.get(this.queueName, 10000L, false, new GetResponseHandler() {
            public void onSuccess(byte[] data) {
                try {
                    synchronized (passed) {
                        passed[0] = true;
                    }
                } finally {
                    readLatch.countDown();
                }
            }

            public void onError(String type, String message) {
                System.out.println("Error on set :(");
            }
        });

        readLatch.await(2, SECONDS);

        synchronized (passed) {
            assertTrue(passed[0]);
        }
    }

    @Test
    public void testPeek() throws Throwable {
        final CountDownLatch writeLatch = new CountDownLatch(1);
        final byte[] value = randomAlphanumeric(128).getBytes();

        final boolean[] passed = new boolean[] { false };

        this.client.set(this.queueName, 0L, value, new SetResponseHandler() {
            public void onSuccess() {
                try {
                    synchronized (passed) { passed[0] = true; }
                } finally {
                    writeLatch.countDown();
                }
            }

            public void onError(String type, String message) { ; }
        });

        writeLatch.await(2, SECONDS);

        synchronized (passed) { assertTrue(passed[0]); }

        synchronized (passed) { passed[0] = false; }

        final CountDownLatch readLatch = new CountDownLatch(1);
        this.client.peek(this.queueName, 0L, new GetResponseHandler() {
            public void onSuccess(byte[] data) {
                try {
                    synchronized (passed) { passed[0] = true; }
                } finally {
                    readLatch.countDown();
                }
            }

            public void onError(String type, String message) { ; }
        });

        readLatch.await(2, SECONDS);

        synchronized (passed) { assertTrue(passed[0]); }
    }

    @Test
    public void testReliableGet() throws Throwable {
        final CountDownLatch writeLatch = new CountDownLatch(1);
        final byte[] value = randomAlphanumeric(128).getBytes();

        final boolean[] passed = new boolean[] { false };

        this.client.set(this.queueName, 0L, value, new SetResponseHandler() {
            public void onSuccess() {
                try {
                    synchronized (passed) { passed[0] = true; }
                } finally {
                    writeLatch.countDown();
                }
            }

            public void onError(String type, String message) {
                System.out.println("Error on set :(");
            }
        });

        writeLatch.await(2, SECONDS);

        synchronized (passed) { assertTrue(passed[0]); }

        synchronized (passed) { passed[0] = false; }

        final CountDownLatch readLatch = new CountDownLatch(1);
        this.client.get(this.queueName, 0L, true, new GetResponseHandler() {
            public void onSuccess(byte[] data) {
                try {
                    synchronized (passed) { passed[0] = true; }
                } finally {
                    readLatch.countDown();
                }
            }

            public void onError(String type, String message) {
                System.out.println("Error on set :(");
            }
        });

        readLatch.await(2, SECONDS);

        synchronized (passed) { assertTrue(passed[0]); }
    }

    @Test
    public void testSetAndForgetRandomLoadWithThreadPool() throws Throwable {
        final int itersPerThread = 100;
        final int producerThreadCount = 4;
        final String qn = this.queueName;

        ExecutorService executor = Executors.newFixedThreadPool(producerThreadCount*2);

        for (int i = 0; i < producerThreadCount; i++) {
            executor.execute(new Runnable() {
                public void run() {
                    for (int i = 0; i < itersPerThread; i++) {
                        client.setAndForget(qn, 120L, randomAlphabetic(i % 512).getBytes());
                    }
                }
            });
        }

        final CountDownLatch latch = new CountDownLatch(itersPerThread * producerThreadCount);

        for (int i = 0; i < producerThreadCount; i++) {
            executor.execute(new Runnable() {
                public void run() {
                    for (int i = 0; i < itersPerThread; i++) {
                        client.get(qn, 2000L, false, new GetResponseHandler() {
                            public void onSuccess(byte[] data) {
                                latch.countDown();
                            }

                            public void onError(String type, String message) {
                                System.out.println(String.format(
                                        "ERROR [%s] [%s]", type, message));
                                latch.countDown();
                            }
                        });
                    }
                }
            });
        }

        this.awaitCountDownLatch(latch, 10, SECONDS);
        assertEquals(0, latch.getCount());

        final boolean[] passed = new boolean[] { false };
        final CountDownLatch statLatch = new CountDownLatch(1);
        this.client.stats(new StatsResponseHandler() {
            public void onSuccess(Collection<QueueStats> stats) {
                for (QueueStats s : stats) {
                    if (queueName.equals(s.getName())) {
                        if ((s.getItems() + s.getTotalItems())== 0) {
                            passed[0] = true;
                        }
                    }
                }
                statLatch.countDown();
            }

            public void onError(String type, String message) {
                System.out.println(String.format("ERROR [%s] [%s]", type, message));
                statLatch.countDown();
            }
        });

        statLatch.await(8, SECONDS);
        assertTrue(passed[0]);
    }

    @Test
    public void testSetAndForgetRandomLoad() throws Throwable {
        int iters = 100000;

        final Set<String> values = new HashSet<String>();
        for (int i = 0; i < iters; i++) {
            while (!values.add(randomAlphabetic(128)));
        }

        assertEquals(iters, values.size());

        for (String v : values) {
            client.setAndForget(this.queueName, 120L, v.getBytes());
        }

        final CountDownLatch latch = new CountDownLatch(iters);
        for (int i = 0; i < iters; i++) {
            client.get(this.queueName, 2000L, false, new GetResponseHandler() {
                public void onSuccess(byte[] data) {
                    try {
                        synchronized (values) {
                            values.remove(new String(data));
                        }
                    } finally {
                        latch.countDown();
                    }
                }

                public void onError(String type, String message) {
                    System.out.println(String.format("ERROR [%s] [%s]", type, message));
                    latch.countDown();
                }
            });
        }

        this.awaitCountDownLatch(latch, 2, MINUTES);

        assertEquals(0, latch.getCount());

        synchronized (values) { assertEquals(0, values.size()); }
    }

    @Test
    public void testSetAndForgetLoad() throws Throwable {
        final byte[] value = randomAlphanumeric(128).getBytes();

        final boolean[] passed = new boolean[] { true };

        for (int i = 0; i < 10000; i++) {
            client.setAndForget(this.queueName, 120L, value);
        }

        final CountDownLatch latch = new CountDownLatch(10000);
        for (int i = 0; i < 10000; i++) {
            client.get(this.queueName, 2000L, false, new GetResponseHandler() {
                public void onSuccess(byte[] data) {
                    try {
                        if (!Arrays.equals(data, value)) {
                            System.out.println(String.format("Data is not equal [%s] [%s]",
                                    new String(data), new String(value)));
                            synchronized (passed) { passed[0] = false; }
                        }
                    } finally {
                        latch.countDown();
                    }
                }

                public void onError(String type, String message) {
                    System.out.println(String.format("ERROR [%s] [%s]", type, message));
                    synchronized (passed) { passed[0] = false; }
                    latch.countDown();
                }
            });
        }

        latch.await();

        synchronized (passed) { assertTrue(passed[0]); }
    }

    private void awaitCountDownLatch(CountDownLatch latch, long timeout, TimeUnit timeoutUnit)
        throws Exception
    {
        long start = System.currentTimeMillis();
        long prev = latch.getCount();
        while (true) {
            latch.await(60, SECONDS);

            if (latch.getCount() == 0) { // We're done
                break;
            }

            if (prev == latch.getCount()) { // We're stalled
                System.out.println("STALLED");
                break;
            }

            if ((System.currentTimeMillis() - start) > timeoutUnit.toMillis(timeout)) {
                // Timeout waiting
                System.out.println("TIMEOUT");
                break;
            }

            prev = latch.getCount();
        }
    }
}
