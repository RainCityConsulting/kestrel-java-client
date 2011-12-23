package com.rcc.kestrel.nio;

import com.rcc.kestrel.AsynchronousClient;
import com.rcc.kestrel.ClientException;
import com.rcc.kestrel.async.*;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import static org.apache.commons.pool.impl.GenericObjectPool.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import static java.nio.channels.SelectionKey.*;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import static java.util.concurrent.TimeUnit.*;

public class Client implements AsynchronousClient {
    private static final Logger log = LoggerFactory.getLogger(Client.class);

    private String hostname;
    private int port;
    private int maxWorkQueueSize;
    private int maxSocketPoolSize;

    private BlockingQueue<WorkItem> workQueue;
    private BlockingQueue<WorkItem> closeGetWorkQueue;
    private SocketWorker socketWorker;

    public Client() { ; }

    public Client(String hostname, int port, int maxWorkQueueSize, int maxSocketPoolSize) {
        this.hostname = hostname;
        this.port = port;
        this.maxWorkQueueSize = maxWorkQueueSize;
        this.maxSocketPoolSize = maxSocketPoolSize;
    }

    public void init() throws IOException {

        this.socketWorker = new SocketWorker(
                this.hostname, this.port, this.maxSocketPoolSize, this);

        Thread thread = new Thread(socketWorker);
        thread.setDaemon(true);
        thread.start();

        this.workQueue = new ArrayBlockingQueue(this.maxWorkQueueSize);
        this.closeGetWorkQueue = new LinkedBlockingQueue(this.maxWorkQueueSize);

        Thread queueWorkerThread = new Thread(
                new QueueWorker(workQueue, socketWorker),
                "Kestrel Work Queue");
        queueWorkerThread.setDaemon(true);
        queueWorkerThread.start();

        // This is needed to guarantee that all gets can be closed (if necessary)
        Thread cgQueueWorkerThread = new Thread(
                new QueueWorker(closeGetWorkQueue, socketWorker),
                "Kestrel Close Get Work Queue");
        cgQueueWorkerThread.setDaemon(true);
        cgQueueWorkerThread.start();
    }

    public void connect() { ; }

    public void disconnect() { ; }

    public boolean stats(StatsResponseHandler handler) {
        return this.workQueue.offer(WorkItem.dumpStats(
                "dump_stats\r\n".getBytes(), new InternalDumpStatsHandler(handler)));
    }

    public boolean setAndForget(String queueName, long expiration, byte[] data) {
        return set(queueName, expiration, data, new LoggingSetResponseHandler());
    }

    public boolean set(String queueName, long expiration, byte[] data, SetResponseHandler handler)
    {
        StringBuilder buf = new StringBuilder();
        buf.append("set ").append(queueName).append(" 0 ");
        if (expiration > 0) {
            buf.append(expiration);
        } else {
            buf.append(0);
        }
        buf.append(" ").append(data.length).append("\r\n");

        String str = buf.toString();
        ByteBuffer bb = ByteBuffer.allocate(str.length() + data.length + 2);
        bb.put(str.getBytes());
        bb.put(data);
        bb.put("\r\n".getBytes());

        return this.workQueue.offer(WorkItem.set(bb.array(), new InternalSetHandler(handler)));
    }

    public boolean get(
            String queueName, long timeoutMs, boolean reliable, GetResponseHandler handler)
    {
        StringBuilder buf = new StringBuilder();
        buf.append("get ").append(queueName);
        if (timeoutMs > 0) {
            buf.append("/t=").append(timeoutMs);
        }
        if (reliable) {
            buf.append("/open");
        }
        buf.append("\r\n");

        log.trace("Sending to server [{}]", buf.toString());

        return this.workQueue.offer(WorkItem.get(
                buf.toString().getBytes(),
                reliable,
                new InternalGetHandler(reliable, this, handler)));
    }

    public boolean peek(String queueName, long timeoutMs, GetResponseHandler handler) {
        StringBuilder buf = new StringBuilder();
        buf.append("get ").append(queueName).append("/peek");
        if (timeoutMs > 0) {
            buf.append("/t=").append(timeoutMs);
        }
        buf.append("\r\n");

        log.trace("Sending to server [{}]", buf.toString());

        return this.workQueue.offer(WorkItem.get(
                buf.toString().getBytes(), false, new InternalGetHandler(false, this, handler)));
    }

    public boolean deleteQueue(String queueName, DeleteResponseHandler handler) {
        log.trace("deleteQueue");

        StringBuilder buf = new StringBuilder();
        buf.append("delete ").append(queueName).append("\r\n");
        return this.workQueue.offer(
                WorkItem.delete(buf.toString().getBytes(), new InternalDeleteHandler(handler)));
    }

    void closeGet(String queueName) {
        log.trace("closeGet");
        StringBuilder buf = new StringBuilder();
        buf.append("get ").append(queueName).append("/close").append("\r\n");
        if (!this.closeGetWorkQueue.offer(WorkItem.maybeSend(
                buf.toString().getBytes(), new InternalCloseGetHandler())))
        {
            throw new RuntimeException("Could not add a close get work item to work queue");
        }
    }

    boolean restart(WorkItem item) {
        return this.workQueue.offer(item);
    }

    private static class QueueWorker implements Runnable {
        private BlockingQueue<WorkItem> queue;
        private SocketWorker socketWorker;

        public QueueWorker(BlockingQueue<WorkItem> queue, SocketWorker socketWorker) {
            this.queue = queue;
            this.socketWorker = socketWorker;
        }

        public void run() {
            while (true) {
                try {
                    WorkItem item = this.queue.take();
                    try {
                        this.socketWorker.submit(item);
                    } catch (SocketPoolExhaustedException e) {
                        log.trace("Socket pool exhausted");
                        log.trace("Adding item back to work queue for future processing");
                        if (!this.queue.offer(item)) {
                            log.warn("Could not add item back to queue because queue is full");
                            item.getHandler().onError(
                                    "OVERLOADED",
                                    "Could not add item back to queue because queue is full");
                        }
                    }
                } catch (Throwable t) {
                    log.error("Caught throwable processing work queue [{}]", t.getMessage(), t);
                }
            }
        }
    }

    private static class SocketWorker implements Runnable {
        private Client client;
        private String hostname;
        private int port;
        private ObjectPool socketPool;

        private ByteBuffer readBuf = ByteBuffer.allocate(1024);
        private List<RegisterRequest> registers = new ArrayList<RegisterRequest>();
        private List<ChangeRequest> changes = new ArrayList<ChangeRequest>();

        private Map<SocketChannel, ByteBuffer> pendingData =
                new HashMap<SocketChannel, ByteBuffer>();

        private Map<SocketChannel, WorkItem> activeWorkItems = 
                Collections.synchronizedMap(new HashMap<SocketChannel, WorkItem>());

        private Selector selector;

        public SocketWorker(String hostname, int port, int maxSocketPoolSize, Client client)
            throws IOException
        {
            this.hostname = hostname;
            this.port = port;
            this.client = client;

            this.selector = SelectorProvider.provider().openSelector();

            // Create the socket pool
            this.socketPool = new GenericObjectPool(
                    new SocketFactory(),
                    maxSocketPoolSize,
                    WHEN_EXHAUSTED_FAIL,
                    0L,
                    maxSocketPoolSize,
                    true,
                    false);
        }

        public void submit(WorkItem item) throws IOException {
            try {
                log.trace("Submitting [{}]", item.getOp());
                SocketChannel channel = (SocketChannel) this.socketPool.borrowObject();
                if (this.activeWorkItems.put(channel, item) != null) {
                    throw new RuntimeException("Overwrote active work queue item");
                }
                ByteBuffer buf = ByteBuffer.wrap(item.getData());
                synchronized (this.pendingData) {
                    log.trace("Writing to pendingData: " + channel.hashCode());
                    this.pendingData.put(channel, buf);
                }

                this.selector.wakeup();
            } catch (NoSuchElementException e) {
                throw new SocketPoolExhaustedException(e);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new ClientException(e);
            }
        }

        public void run() {
            while (true) {
                try {
                    synchronized (this.registers) {
                        for (RegisterRequest r : this.registers) {
                            log.trace("Registering connection");
                            r.getChannel().register(this.selector, r.getOps());
                        }
                        this.registers.clear();
                    }

                    synchronized (this.changes) {
                        for (ChangeRequest r : this.changes) {
                            log.trace("Changing connection: " + r.getChannel().hashCode());
                            SelectionKey key = r.getChannel().keyFor(this.selector);
                            key.interestOps(r.getOps());
                        }
                        this.changes.clear();
                    }

                    this.selector.select();

                    Iterator<SelectionKey> i = this.selector.selectedKeys().iterator();
                    while (i.hasNext()) {
                        SelectionKey key = i.next();
                        i.remove();
                        if (key.isValid()) {
                            if (key.isConnectable()) {
                                log.trace("Connecting");
                                this.finishConnection(key);
                            } else if (key.isReadable()) {
                                log.trace("Reading");
                                this.read(key);
                            } else if (key.isWritable()) {
                                log.trace("Writing");
                                this.write(key);
                            }
                        }
                    }
                } catch (SocketClosedException e) {
                    try {
                        log.debug("Socket was reset by peer");
                        WorkItem item = null;
                        item = this.activeWorkItems.remove(e.getChannel());
                        if (item == null) {
                            throw new RuntimeException("Work item is null");
                        }
                        this.socketPool.invalidateObject(e.getChannel());
                        this.client.restart(item);
                    } catch (Exception f) {
                        log.error("Caught exception closing reset socket [{}]", f.getMessage(), f);
                    }
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                }
            }
        }

        private void finishConnection(SelectionKey key) {
            try {
                ((SocketChannel) key.channel()).finishConnect();
                key.interestOps(OP_WRITE);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                key.cancel();
            }
        }

        private byte[] readAll(SelectionKey key) throws Exception {
            SocketChannel channel = (SocketChannel) key.channel();
            readBuf.clear();
            int numRead = channel.read(readBuf);

            byte[] data = new byte[0];
            while (numRead == 1024) {
                int start = data.length;
                data = Arrays.copyOf(data, data.length + numRead);
                System.arraycopy(readBuf.array(), 0, data, start, numRead);
                readBuf.clear();
                numRead = channel.read(readBuf);
            }

            if (numRead == -1) {
                key.cancel();
                throw new SocketClosedException(channel);
            } else {
                int start = data.length;
                data = Arrays.copyOf(data, data.length + numRead);
                System.arraycopy(readBuf.array(), 0, data, start, numRead);
            }

            return data;
        }

        private void read(SelectionKey key) throws Exception {
            byte[] data = readAll(key);
            SocketChannel channel = (SocketChannel) key.channel();
            //readBuf.clear();
            //int numRead = channel.read(readBuf);

            //if (numRead == -1) {
                //key.cancel();
                //throw new SocketClosedException(channel);
            //} else {
                //byte[] data = new byte[numRead];
                //System.arraycopy(readBuf.array(), 0, data, 0, numRead);
                WorkItem item = null;
                item = this.activeWorkItems.get(channel);
                if (item == null) {
                    throw new RuntimeException("Work item is null");
                }
                if (item.getHandler() != null) {
                    if (item.getHandler().receiveData(data)) {
                        key.interestOps(OP_WRITE);
                        if (this.activeWorkItems.remove(channel) == null) {
                            throw new RuntimeException("Nothing removed off of work queue");
                        }
                        this.socketPool.returnObject(channel);
                    } else {
                        log.debug("Read partial data");
                    }
                } else {
                    // Not sure when to close the connection, so do it right away
                    key.cancel();
                    if (this.activeWorkItems.remove(channel) == null) {
                        throw new RuntimeException("Nothing removed off of work queue");
                    }
                    this.socketPool.returnObject(channel);
                }
            //}
        }

        private void write(SelectionKey key) throws IOException {
            //log.debug("write0000");
            SocketChannel channel = (SocketChannel) key.channel();
            synchronized (this.pendingData) {
                log.trace("Reading from pendingData [{}]", channel.hashCode());
                ByteBuffer buf = this.pendingData.get(channel);
                if (buf != null) {
                    channel.write(buf);
                    if (buf.remaining() == 0) {
                        this.pendingData.remove(channel);
                        key.interestOps(OP_READ);
                    } else {
                        log.trace("Did not write all of buffer to channel [{}] remaining", 
                                buf.remaining());
                    }
                } else {
                    log.trace("not writing - buf is null");
                }
            }
        }

        private static class RegisterRequest {
            private SocketChannel channel;
            private int ops;

            public RegisterRequest(SocketChannel channel, int ops) {
                this.channel = channel;
                this.ops = ops;
            }

            public SocketChannel getChannel() { return this.channel; }
            public int getOps() { return this.ops; }
        }

        private static class ChangeRequest {
            private SocketChannel channel;
            private int ops;

            public ChangeRequest(SocketChannel channel, int ops) {
                this.channel = channel;
                this.ops = ops;
            }

            public SocketChannel getChannel() { return this.channel; }
            public int getOps() { return this.ops; }
        }

        private static class SocketClosedException extends RuntimeException {
            private SocketChannel channel;

            public SocketClosedException(SocketChannel channel) {
                this.channel = channel;
            }

            public SocketChannel getChannel() {
                return this.channel;
            }
        }

        private class SocketFactory extends BasePoolableObjectFactory {

            public Object makeObject() throws Exception {
                SocketChannel channel = SocketChannel.open();
                channel.configureBlocking(false);
                channel.connect(new InetSocketAddress(hostname, port));
                synchronized (registers) {
                    registers.add(new RegisterRequest(channel, OP_CONNECT));
                }
                log.trace("Waking up selector - initConnection");
                selector.wakeup();
                return channel;
            }

            public void destroyObject(Object obj) throws Exception {
                /*
                try {
                    if (true) throw new RuntimeException();
                } catch (RuntimeException e) {
                    log.debug("Destroying socket channel", e);
                }
                */
                log.debug("Destroying socket channel");
                SocketChannel channel = (SocketChannel) obj;
                channel.close();
            }

            public boolean validateObject(Object obj) {
                SocketChannel channel = (SocketChannel) obj;
                boolean ret = channel.isConnected() || channel.isConnectionPending();
                if (!ret) { log.debug("Socket channel is not connected"); }
                return ret;
            }
        }
    }

    // Setters

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setMaxWorkQueueSize(int maxWorkQueueSize) {
        this.maxWorkQueueSize = maxWorkQueueSize;
    }

    public void setMaxSocketPoolSize(int maxSocketPoolSize) {
        this.maxSocketPoolSize = maxSocketPoolSize;
    }

    private static class SocketPoolExhaustedException extends RuntimeException {
        public SocketPoolExhaustedException(Throwable cause) {
            super(cause);
        }
    }

    public static class OverloadedException extends RuntimeException {
        public OverloadedException(String message) {
            super(message);
        }
    }

    private static class WorkItem {
        public static enum Op { DELETE, DUMP_STATS, GET, MAYBE_SEND, SET; }

        private Op op;
        private byte[] data;
        private InternalHandler handler;
        private boolean isReliable;

        public static WorkItem dumpStats(byte[] data, InternalHandler handler) {
            return new WorkItem(Op.DUMP_STATS, data, handler); 
        }

        public static WorkItem maybeSend(byte[] data, InternalHandler handler) {
            return new WorkItem(Op.MAYBE_SEND, data, handler); 
        }

        public static WorkItem get(byte[] data, boolean isReliable, InternalHandler handler) {
            return new WorkItem(Op.GET, data, isReliable, handler); 
        }

        public static WorkItem set(byte[] data, InternalHandler handler) {
            return new WorkItem(Op.SET, data, handler); 
        }

        public static WorkItem delete(byte[] data, InternalHandler handler) {
            return new WorkItem(Op.DELETE, data, handler); 
        }

        private WorkItem(Op op, byte[] data, InternalHandler handler) {
            this(op, data, false, handler);
        }

        private WorkItem(Op op, byte[] data, boolean isReliable, InternalHandler handler) {
            this.op = op;
            this.data = data;
            this.handler = handler;
            this.isReliable = isReliable;
        }

        public Op getOp() { return this.op; }
        public byte[] getData() { return this.data; }
        public boolean isReliable() { return this.isReliable; }
        public InternalHandler getHandler() { return this.handler; }
    }
}
