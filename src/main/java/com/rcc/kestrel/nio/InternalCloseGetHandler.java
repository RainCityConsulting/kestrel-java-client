package com.rcc.kestrel.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

class InternalCloseGetHandler extends InternalErrorHandler {
    private static final Logger log = LoggerFactory.getLogger(InternalCloseGetHandler.class);

    protected boolean accumulate(byte[] acc) {
        if (Arrays.equals("END\r\n".getBytes(), acc)) {
            return true;
        }
        return false;
    }

    public void onError(String type, String message) {
        log.error("Error closing get: " + type + ": " + message);
    }
}
