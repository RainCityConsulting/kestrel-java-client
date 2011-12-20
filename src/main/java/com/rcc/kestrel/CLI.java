package com.rcc.kestrel;

import com.rcc.kestrel.async.GetResponseHandler;
import com.rcc.kestrel.nio.Client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.text.ParseException;

public class CLI {
    private static final Logger log = LoggerFactory.getLogger(CLI.class);

    public static void main(String[] args) {
        System.out.println(System.getProperty("log4j.configuration"));
        try {
            ApplicationContext ctx = new ClassPathXmlApplicationContext("/event-monitor-ctx.xml");

            SynchronousClient client =
                    (SynchronousClient) ctx.getBean("nioSynchronousKestrelClient");

            String action = getRequiredArg("kestrel.action");

            if ("peek".equals(action)) {
                long timeout = (long) getIntArg("kestrel.timeout", 0);
                String queueName = args[0];
                byte[] data = client.peek("method_calls", timeout);
                log.info("Received {} bytes of data", data.length);
            } else if ("get".equals(action)) {
                long timeout = (long) getIntArg("kestrel.timeout", 0);
                boolean isReliable = Boolean.getBoolean("kestrel.reliable");
                String queueName = args[0];
                byte[] data = client.get("method_calls", timeout, isReliable);
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

    private static int getIntArg(String name, int fallback) throws ParseException {
        String v = System.getProperty(name);
        if (v == null || v.length() == 0) {
            return fallback;
        } else {
            return Integer.parseInt(v);
        }
    }
}
