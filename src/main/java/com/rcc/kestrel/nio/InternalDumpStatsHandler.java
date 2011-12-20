package com.rcc.kestrel.nio;

import com.rcc.kestrel.QueueStats;
import com.rcc.kestrel.async.StatsResponseHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class InternalDumpStatsHandler extends InternalErrorHandler {
    private static final Pattern QUEUE = Pattern.compile("^queue '(.*)' \\{$");
    private static final Pattern NVP = Pattern.compile("^\\p{Space}*(\\p{Alpha})=(\\p{Digit})$");

    private static final Logger log = LoggerFactory.getLogger(InternalDumpStatsHandler.class);

    private StatsResponseHandler statsResponseHandler;

    public InternalDumpStatsHandler(StatsResponseHandler statsResponseHandler) {
        this.statsResponseHandler = statsResponseHandler;
    }

    protected boolean accumulate(byte[] acc) {
        int l = acc.length;
        log.info("Accumulated [{}]", new String(acc));
        if (l >= 5
                && acc[l-5] == 'E'
                && acc[l-4] == 'N'
                && acc[l-3] == 'D'
                && acc[l-2] == '\r'
                && acc[l-1] == '\n')
        { // We've read to the end
            List<QueueStats> stats = new ArrayList<QueueStats>();
            try {
                int pos = 0;
                String line = readToEndOfLine(acc, pos);
                pos += line.length();
                while (!"END\r\n".equals(line)) {
                    System.out.println(String.format("line: [%s]", line));
                    long items = 0l;
                    long bytes = 0l;
                    long totalItems = 0l;
                    long logSize = 0l;
                    long expiredItems = 0l;
                    long memItems = 0l;
                    long memBytes = 0l;
                    long age = 0l;
                    long discarded = 0l;
                    long waiters = 0l;
                    int openTxns = 0;

                    Matcher m = QUEUE.matcher(line);
                    if (m.matches()) {
                        String queueName = m.group(1);
                        String nvpLine = readToEndOfLine(acc, pos);
                        pos += line.length();
                        if ("}\r\n".equals(nvpLine)) { // End
                            stats.add(new QueueStats(
                                    queueName, items, bytes, totalItems, logSize, expiredItems,
                                    memItems, memBytes, age, discarded, waiters, openTxns));
                        } else {
                            m = NVP.matcher(nvpLine);
                            if (m.matches()) {
                                String n = m.group(1);
                                String v = m.group(2);
                                if ("items".equals(n)) {
                                    items = Long.parseLong(v);
                                } else if ("bytes".equals(n)) {
                                    bytes = Long.parseLong(v);
                                } else if ("totalItems".equals(n)) {
                                    totalItems = Long.parseLong(v);
                                } else if ("logSize".equals(n)) {
                                    logSize = Long.parseLong(v);
                                } else if ("expiredItems".equals(n)) {
                                    expiredItems = Long.parseLong(v);
                                } else if ("memItems".equals(n)) {
                                    memItems = Long.parseLong(v);
                                } else if ("memBytes".equals(n)) {
                                    memBytes = Long.parseLong(v);
                                } else if ("age".equals(n)) {
                                    age = Long.parseLong(v);
                                } else if ("discarded".equals(n)) {
                                    discarded = Long.parseLong(v);
                                } else if ("waiters".equals(n)) {
                                    waiters = Long.parseLong(v);
                                } else if ("openTransactions".equals(n)) {
                                    openTxns = Integer.parseInt(v);
                                } else {
                                    throw new RuntimeException(String.format(
                                            "Unexpected NVP name [%s]", n));
                                }
                            } else {
                                throw new RuntimeException(
                                        String.format("Unexpected value from server [%s]", line));
                            }
                        }
                    } else {
                        throw new RuntimeException(
                                String.format("Unexpected value from server [%s]", line));
                    }

                    line = readToEndOfLine(acc, pos);
                    pos += line.length();
                }
                this.statsResponseHandler.onSuccess(stats);
            } catch (Throwable t) {
                log.warn("Unexpected Throwable [{}]", t.getMessage(), t);
            } finally {
                return true;
            }
        }

        // Keep going
        return false;
    }

    public void onError(String type, String message) {
        this.statsResponseHandler.onError(type, message);
    }
}
