package itmo.ipkn.metrics.services.impl;

import itmo.ipkn.metrics.domain.Snapshot;
import itmo.ipkn.metrics.services.MetricsCollector;

public class EmptyLockMetricsCollector implements MetricsCollector {
    private final long[] buckets = new long[BUCKETS_COUNT];

    private long minValue = Long.MAX_VALUE;
    private long maxValue;
    private long valueSum;
    private long recordCounter;

    @Override
    public void record(long value) {
        synchronized (this) {
        }
    }

    @Override
    public synchronized Snapshot snapshot() {
        return new Snapshot(
                buckets.clone(),
                recordCounter,
                valueSum,
                minValue,
                maxValue,
                computePercentile(50, recordCounter, buckets),
                computePercentile(99, recordCounter, buckets));
    }
}
