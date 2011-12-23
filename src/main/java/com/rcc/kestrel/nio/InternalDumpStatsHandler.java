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
    private static final Pattern QUEUE = Pattern.compile("^queue \\'(.*)\\' \\{\\p{Space}*$");

    private static final Pattern NVP =
            Pattern.compile("^\\p{Space}*(\\p{Alpha}+)=(\\p{Digit}+)\\p{Space}*$");

    private static final Logger log = LoggerFactory.getLogger(InternalDumpStatsHandler.class);

    private StatsResponseHandler statsResponseHandler;

    public InternalDumpStatsHandler(StatsResponseHandler statsResponseHandler) {
        this.statsResponseHandler = statsResponseHandler;
    }

    protected boolean accumulate(byte[] acc) {
        int l = acc.length;
        if (l >= 5
                && acc[l-5] == 'E'
                && acc[l-4] == 'N'
                && acc[l-3] == 'D'
                && acc[l-2] == '\r'
                && acc[l-1] == '\n')
        { // We've read to the end
            String[] lines = (new String(acc)).split("\\r\\n");
            List<QueueStats> stats = new ArrayList<QueueStats>();
            int i = 0;
            while (true) {
                String line = lines[i];
                Matcher m = QUEUE.matcher(line);
                while ((i+1) < lines.length && !m.matches()) {
                    line = lines[++i];
                    m = QUEUE.matcher(line);
                }

                if (!m.matches()) { break; }

                String queueName = m.group(1);
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

                line = lines[++i];
                m = NVP.matcher(line);
                while (m.matches()) {
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

                    line = lines[++i];
                    m = NVP.matcher(line);
                }

                stats.add(new QueueStats(
                        queueName, items, bytes, totalItems, logSize, expiredItems,
                        memItems, memBytes, age, discarded, waiters, openTxns));
            }

            try {
                this.statsResponseHandler.onSuccess(stats);
            } catch (Throwable t) {
                log.warn("Caught Throwable calling responseHandler.onSuccess [{}]",
                        t.getMessage(), t);
            }

            return true;
        }

        // Keep going
        return false;
    }

    public void onError(String type, String message) {
        this.statsResponseHandler.onError(type, message);
    }

    private String readToEndOfLine(byte[] data, int pos) {
        int end = pos;
        while ((end+1) < data.length && data[end] != '\r' && data[end+1] != '\n') end++;

        /*
        int len = 0;
        if ((end+1) < data.length && data[end] == '\r' && data[end+1] == '\n') {
        } else {
        }
        */
        int len = end - pos;

        byte[] tmp = new byte[len];
        System.arraycopy(data, pos, tmp, 0, len);
        return new String(tmp);
    }
}
