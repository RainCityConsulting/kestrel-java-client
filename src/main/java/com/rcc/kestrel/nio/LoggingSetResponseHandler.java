package com.rcc.kestrel.nio;

import com.rcc.kestrel.async.SetResponseHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingSetResponseHandler implements SetResponseHandler {
    private static final Logger log = LoggerFactory.getLogger(LoggingSetResponseHandler.class);

    public void onSuccess() {
        log.trace("Successful set");
    }
    public void onError(String type, String message) {
        log.error("Error setting [{}] [{}]", type, message);
    }
}
