package itmo.ipkn.metrics.services.impl;

import itmo.ipkn.metrics.domain.Snapshot;
import itmo.ipkn.metrics.domain.ThreadBuffers;
import itmo.ipkn.metrics.services.MetricsCollector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class BufferedMetricsCollector implements MetricsCollector {

    private final List<ThreadBuffers> threadBuffers = new ArrayList<>();
    private final Object mutex = new Object();
    private final ThreadLocal<ThreadBuffers> localBuffers = ThreadLocal.withInitial(() -> {
        ThreadBuffers s = ThreadBuffers.of();
        synchronized (mutex) {
            threadBuffers.add(s);
        }
        return s;
    });

    private long minValue = Long.MAX_VALUE;
    private long maxValue;
    private long valueSum;
    private long recordCounter;
    private final long[] buckets = new long[BUCKETS_COUNT];

    private volatile int active = 0;

    @Override
    public void record(long value) {
        ThreadBuffers buffers = localBuffers.get();
        int bufferNumber;
        while (true) {
            bufferNumber = active;
            buffers.inside().set(bufferNumber);
            if (active == bufferNumber) {
                break;
            }
            buffers.inside().set(bufferNumber);
        }

        int bucketIdx = toIntExact(min(value / BUCKETS_INTERVAL, BUCKETS_COUNT - 1));
        buffers.buckets()[bufferNumber][bucketIdx]++;
        buffers.count()[bufferNumber]++;
        buffers.sum()[bufferNumber] += value;
        buffers.min()[bufferNumber] = min(buffers.min()[bufferNumber], value);
        buffers.max()[bufferNumber] = max(buffers.max()[bufferNumber], value);

        buffers.inside().setRelease(-1);
    }

    @Override
    public Snapshot snapshot() {
        synchronized (mutex) {
            var old = active;
            active = 1 - old;
            for (ThreadBuffers buffers : threadBuffers) {
                while (buffers.inside().get() == old) {
                    Thread.onSpinWait();
                }
                recordCounter += buffers.count()[old];
                valueSum += buffers.sum()[old];
                minValue = min(buffers.min()[old], minValue);
                maxValue = max(buffers.max()[old], maxValue);
                for (int i = 0; i < BUCKETS_COUNT; i++) {
                    buckets[i] += buffers.buckets()[old][i];
                }
                buffers.count()[old] = 0;
                buffers.sum()[old] = 0;
                buffers.max()[old] = 0;
                buffers.min()[old] = Long.MAX_VALUE;
                Arrays.fill(buffers.buckets()[old], 0);
            }

            long p50 = computePercentile(50, recordCounter, buckets);
            long p99 = computePercentile(99, recordCounter, buckets);

            return new Snapshot(buckets, recordCounter, valueSum, minValue, maxValue, p50, p99);
        }
    }
}
