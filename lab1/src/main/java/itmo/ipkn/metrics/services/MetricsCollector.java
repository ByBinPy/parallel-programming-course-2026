package itmo.ipkn.metrics.services;

import itmo.ipkn.metrics.domain.Snapshot;

public interface MetricsCollector {

    int BUCKETS_COUNT = 256;
    int BUCKETS_INTERVAL = 4;

    void record(long value);

    Snapshot snapshot();

    default long computePercentile(int percent, long recordCounter, long[] buckets) {
        double threshold = ((double) (recordCounter * percent)) / 100;
        long acc = 0;
        for (int bucketNumber = 0; bucketNumber < BUCKETS_COUNT; bucketNumber++) {
            acc += buckets[bucketNumber];
            if (acc > threshold) {
                return bucketNumber * BUCKETS_INTERVAL;
            }
        }

        return BUCKETS_COUNT * BUCKETS_INTERVAL;
    }
}