package com.rcc.kestrel;

import com.rcc.kestrel.async.GetResponseHandler;
import com.rcc.kestrel.nio.Client;
import com.rcc.kestrel.nio.SynchronousClient;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Collection;

public class CLI {
    private static final Logger log = LoggerFactory.getLogger(CLI.class);

    public static void main(String[] args) {
        try {
            String hostname = getRequiredArg("kestrel.server.hostname");
            int port = getRequiredIntArg("kestrel.server.port");
            boolean prettyPrint = Boolean.getBoolean("kestrel.prettyPrint");

            SynchronousClient client = new SynchronousClient(hostname, port);
            client.init();

            String action = getRequiredArg("kestrel.action");

            if ("list-queues".equals(action)) {
                Collection<QueueStats> stats = client.stats();
                for (QueueStats s : stats) {
                    System.out.println(s.getName());
                }
            } else if ("queue-stats".equals(action)) {
                Gson gson = null;
                if (prettyPrint) {
                    gson = new GsonBuilder().setPrettyPrinting().create();
                } else {
                    gson = new GsonBuilder().create();
                }

                Collection<QueueStats> stats = client.stats();
                for (String q : args) {
                    for (QueueStats s : stats) {
                        if (q.equals(s.getName())) {
                            System.out.println(gson.toJson(s));
                        }
                    }
                }
            } else if ("delete-queue".equals(action)) {
                for (String q : args) {
                    if (client.deleteQueue(q)) {
                        System.err.println(String.format("Queue [%s] has been deleted", q));
                    } else {
                        System.err.println(String.format("Queue [%s] was NOT deleted", q));
                    }
                }
            } else if ("peek".equals(action)) {
                long timeout = (long) getIntArg("kestrel.timeout", 0);
                String queueName = args[0];
                byte[] data = client.peek(queueName, timeout);
                log.info("Received {} bytes of data", data.length);
            } else if ("get".equals(action)) {
                long timeout = (long) getIntArg("kestrel.timeout", 0);
                boolean isReliable = Boolean.getBoolean("kestrel.reliable");
                String queueName = args[0];
                byte[] data = client.get(queueName, timeout, isReliable);
                log.info("Received {} bytes of data", data.length);
            }
        } catch (Throwable t) {
            log.error("Unexpected error [{}]", t.getMessage(), t);
            System.exit(1);
        }
    }

    private static String getRequiredArg(String name) {
        String v = System.getProperty(name);
        if (v == null || v.length() == 0) {
            throw new RuntimeException("Required system property is missing: " + name);
        }
        return v;
    }

    private static int getRequiredIntArg(String name) throws ParseException {
        String v = System.getProperty(name);
        if (v == null || v.length() == 0) {
            throw new RuntimeException(
                    String.format("Required system property is missing [%s]", name));
        } else {
            return Integer.parseInt(v);
        }
    }

    private static int getIntArg(String name, int fallback) throws ParseException {
        String v = System.getProperty(name);
        if (v == null || v.length() == 0) {
            return fallback;
        } else {
            return Integer.parseInt(v);
        }
    }
}
