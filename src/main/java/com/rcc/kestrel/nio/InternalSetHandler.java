package com.rcc.kestrel.nio;

import com.rcc.kestrel.async.SetResponseHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

class InternalSetHandler extends InternalErrorHandler {
    private static final Logger log = LoggerFactory.getLogger(InternalSetHandler.class);

    private SetResponseHandler setResponseHandler;

    public InternalSetHandler(SetResponseHandler setResponseHandler) {
        this.setResponseHandler = setResponseHandler;
    }

    protected boolean accumulate(byte[] acc) {
        if (Arrays.equals("STORED\r\n".getBytes(), acc)) {
            if (this.setResponseHandler != null) {
                this.setResponseHandler.onSuccess();
            }
            return true;
        }
        return false;
    }

    public void onError(String type, String message) {
        if (this.setResponseHandler != null) {
            this.setResponseHandler.onError(type, message);
        }
    }
}
