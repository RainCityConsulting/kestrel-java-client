package com.rcc.kestrel.nio;

public interface InternalHandler {
    public boolean receiveData(byte[] data);
    public void onError(String type, String message);
}
