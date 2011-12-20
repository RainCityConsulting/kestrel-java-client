package com.rcc.kestrel.nio;

import java.util.Arrays;

abstract class InternalErrorHandler implements InternalHandler {
    private static final byte[] ERROR = "ERROR\r\n".getBytes();
    private static final byte[] CLIENT_ERROR = "CLIENT_ERROR ".getBytes();
    private static final byte[] SERVER_ERROR = "SERVER_ERROR ".getBytes();

    private byte[] acc = new byte[0];
    private boolean isClientError = false;
    private boolean isServerError = false;

    public boolean receiveData(byte[] data) {
        if (acc.length == 0) {
            acc = data;
        } else {
            byte[] tmp = Arrays.copyOf(acc, acc.length + data.length);
            System.arraycopy(data, 0, tmp, acc.length, data.length);
        }

        // Check for error
        if (acc.length == 7 && Arrays.equals(ERROR, acc)) {
            this.onError("ERROR", null);
            return true;
        } else if (acc.length >= 15) {
            byte[] tmp = Arrays.copyOf(acc, 13);
            if (Arrays.equals(CLIENT_ERROR, tmp)) {
                this.isClientError = true;
                // Check if we've received all the data
                int l = acc.length;
                if (acc[l-2] == '\r' && acc[l-1] == '\n') {
                    this.onError("CLIENT_ERROR", new String(acc));
                }
                return true;
            } else if (Arrays.equals(SERVER_ERROR, tmp)) {
                this.isServerError = true;
                // Check if we've received all the data
                int l = acc.length;
                if (acc[l-2] == '\r' && acc[l-1] == '\n') {
                    this.onError("SERVER_ERROR", new String(acc));
                }
                return true;
            }
        }

        return this.accumulate(acc);
    }

    protected abstract boolean accumulate(byte[] acc);
}
