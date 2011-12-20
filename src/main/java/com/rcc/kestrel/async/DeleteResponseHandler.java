package com.rcc.kestrel.async;

public interface DeleteResponseHandler {
    public void onSuccess();
    public void onError(String type, String message);
}
