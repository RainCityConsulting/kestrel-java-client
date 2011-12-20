package com.rcc.kestrel.async;

import com.rcc.kestrel.QueueStats;

import java.util.Collection;

public interface StatsResponseHandler {
    public void onSuccess(Collection<QueueStats> stats);
    public void onError(String type, String message);
}
