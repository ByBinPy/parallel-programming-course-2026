package itmo.ipkn.metrics.services.impl;

import itmo.ipkn.metrics.domain.Snapshot;
import itmo.ipkn.metrics.services.MetricsCollector;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class SyncMetricsCollector implements MetricsCollector {

    private long minValue;
    private long maxValue;
    private long valueSum;
    private long recordCounter;

    private final long[] buckets = new long[BUCKETS_COUNT];

    @Override
    public synchronized void record(long value) {
        int bucketIdx = toIntExact(min(value / BUCKETS_INTERVAL, BUCKETS_COUNT - 1));
        buckets[bucketIdx]++;
        recordCounter++;
        valueSum += value;
        minValue = min(value, minValue);
        maxValue = max(value, maxValue);
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
