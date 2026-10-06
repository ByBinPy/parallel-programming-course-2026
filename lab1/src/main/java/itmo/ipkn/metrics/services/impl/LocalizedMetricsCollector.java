package itmo.ipkn.metrics.services.impl;

import itmo.ipkn.metrics.domain.Snapshot;
import itmo.ipkn.metrics.domain.ThreadState;
import itmo.ipkn.metrics.services.MetricsCollector;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class LocalizedMetricsCollector implements MetricsCollector {
    private final List<ThreadState> threadStates = new ArrayList<>();
    private final Object mutex = new Object();
    private final ThreadLocal<ThreadState> localState = ThreadLocal.withInitial(() -> {
        ThreadState s = ThreadState.of();
        synchronized (mutex) {
            threadStates.add(s);
        }
        return s;
    });

    @Override
    public void record(long value) {
        ThreadState threadState = localState.get();
        int bucketIdx = toIntExact(min(value / BUCKETS_INTERVAL, BUCKETS_COUNT - 1));
        threadState.buckets().setRelease(bucketIdx, threadState.buckets().getPlain(bucketIdx) + 1);
        threadState.count().setRelease(threadState.count().getPlain() + 1);
        threadState.sum().setRelease(threadState.sum().getPlain() + value);
        if (value < threadState.min().getPlain()) {
            threadState.min().setRelease(value);
        }
        if (value > threadState.max().getPlain()) {
            threadState.max().setRelease(value);
        }
    }

    @Override
    public Snapshot snapshot() {
        long[] out = new long[BUCKETS_COUNT];
        long count = 0, sum = 0, min = Long.MAX_VALUE, max = 0;
        List<ThreadState> copyOfStates = threadStates.stream().map(ThreadState::clone).toList();
        for (ThreadState state : copyOfStates) {
            for (int i = 0; i < BUCKETS_COUNT; i++) {
                count += state.count().get();
                sum += state.sum().get();
                min = min(min, state.min().get());
                max = max(max, state.max().get());
            }
        }
        long p50 = computePercentile(50, count, out);
        long p99 = computePercentile(99, count, out);

        return new Snapshot(out, count, sum, min, max, p50, p99);
    }
}
