package com.rcc.kestrel.nio;

public interface ResponseHandler {
    public boolean receiveData(byte[] data);
}
