package com.rcc.kestrel.nio;

import com.rcc.kestrel.async.DeleteResponseHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

class InternalDeleteHandler extends InternalErrorHandler {
    private static final Logger log = LoggerFactory.getLogger(InternalDeleteHandler.class);

    private DeleteResponseHandler deleteResponseHandler;

    public InternalDeleteHandler(DeleteResponseHandler deleteResponseHandler) {
        this.deleteResponseHandler = deleteResponseHandler;
    }

    protected boolean accumulate(byte[] acc) {
        if (Arrays.equals("STORED\r\n".getBytes(), acc)) {
            if (this.deleteResponseHandler != null) {
                this.deleteResponseHandler.onSuccess();
            }
            return true;
        }
        return false;
    }

    public void onError(String type, String message) {
        if (this.deleteResponseHandler != null) {
            this.deleteResponseHandler.onError(type, message);
        }
    }
}
