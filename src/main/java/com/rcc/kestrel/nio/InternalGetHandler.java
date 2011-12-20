package com.rcc.kestrel.nio;

import com.rcc.kestrel.async.GetResponseHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

class InternalGetHandler extends InternalErrorHandler {
    private static final Logger log = LoggerFactory.getLogger(InternalGetHandler.class);

    private static final byte[] VALUE = "VALUE ".getBytes();

    private boolean isReliable;
    private Client client;
    private GetResponseHandler getResponseHandler;

    public InternalGetHandler(
            boolean isReliable,
            Client client,
            GetResponseHandler getResponseHandler)
    {
        this.isReliable = isReliable;
        this.client = client;
        this.getResponseHandler = getResponseHandler;
    }

    protected boolean accumulate(byte[] acc) {
        int l = acc.length;
        log.trace("Accumulated length [{}]", l);
        if (l >= 5
                && acc[l-5] == 'E'
                && acc[l-4] == 'N'
                && acc[l-3] == 'D'
                && acc[l-2] == '\r'
                && acc[l-1] == '\n')
        { // We've read to the end
            try {
                // Check that we have a value
                boolean hasValue = false;
                if (l > 6
                        && acc[0] == 'V'
                        && acc[1] == 'A'
                        && acc[2] == 'L'
                        && acc[3] == 'U'
                        && acc[4] == 'E'
                        && acc[5] == ' ')
                {
                    hasValue = true;
                }

                byte[] data = null;
                String queueName = null;
                if (!hasValue) {
                    data = new byte[0];
                } else {
                    int i = 0;
                    while ((i+1) < l) {
                        if (acc[i] == '\r' && acc[i+1] == '\n') { break; }
                        i++;
                    }
                    byte[] tmp = new byte[i];
                    System.arraycopy(acc, 0, tmp, 0, i);
                    String valueLine = new String(tmp);
                    log.trace("Value line [{}]", valueLine);
                    String[] fields = valueLine.split(" ");
                    queueName = fields[1];
                    String flags = fields[2];
                    int byteCount = Integer.parseInt(fields[3]);
                    data = new byte[byteCount];
                    System.arraycopy(acc, i+2, data, 0, byteCount);
                }

                try {
                    this.getResponseHandler.onSuccess(data);
                } catch (Throwable t) {
                    log.warn("Caught Throwable calling responseHandler.onSuccess [{}]",
                            t.getMessage(), t);
                }

                if (this.isReliable && data.length > 0) {
                    this.client.closeGet(queueName);
                }
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
        this.getResponseHandler.onError(type, message);
    }
}
