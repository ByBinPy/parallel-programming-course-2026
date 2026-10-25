package itmo.ipkn.metrics.services.impl;

import itmo.ipkn.metrics.domain.Snapshot;
import itmo.ipkn.metrics.services.MetricsCollector;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class ShardedMetricsCollectorImpl implements MetricsCollector {

    private static final Integer GROUP_SIZE = 16;

    private final ReentrantLock[] groupLocks;
    private final AtomicLong minValue = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxValue = new AtomicLong();
    private final AtomicLong valueSum = new AtomicLong();
    private final AtomicLong recordCounter = new AtomicLong();
    private final long[] buckets = new long[BUCKETS_COUNT];

    public ShardedMetricsCollectorImpl() {
        groupLocks = new ReentrantLock[GROUP_SIZE];
        for (int i = 0; i < GROUP_SIZE; i++) {
            groupLocks[i] = new ReentrantLock();
        }
    }

    @Override
    public void record(long value) {
        int bucketIdx = toIntExact(min(value / 4, 255));
        int group = toIntExact(bucketIdx % 16);
        ReentrantLock groupLock = groupLocks[group];
        lockAction(groupLock, bucketIdx, (bucketIndex) -> {
            buckets[bucketIndex]++;
        });
        waitAction(() -> {
            long max = maxValue.get();
            return max >= value || maxValue.compareAndSet(max, value);
        });
        waitAction(() -> {
            long min = minValue.get();
            return min <= value || minValue.compareAndSet(min, value);
        });
        valueSum.accumulateAndGet(value, Long::sum);
        recordCounter.getAndIncrement();
    }

    @Override
    public Snapshot snapshot() {
        long[] buffBuckets = new long[BUCKETS_COUNT];
        for (int i = 0; i < GROUP_SIZE; i++) {
            ReentrantLock groupLock = groupLocks[i];
            lockAction(groupLock, i, (groupNumber) -> {
                for (int j = groupNumber; j < BUCKETS_COUNT; j += GROUP_SIZE) {
                    buffBuckets[j] = buckets[j];
                }
            });
        }

        long count = recordCounter.get();
        return new Snapshot(
                buffBuckets,
                count,
                valueSum.get(),
                minValue.get(),
                maxValue.get(),
                computePercentile(50, count, buffBuckets),
                computePercentile(99, count, buffBuckets));
    }

    private <T> void lockAction(ReentrantLock lock, T val, Consumer<T> criticalAction) {
        waitAction(lock::tryLock);
        criticalAction.accept(val);
        lock.unlock();
    }

    private void waitAction(BooleanSupplier breakWaitCondition) {
        while (true) {
            if (breakWaitCondition.getAsBoolean()) {
                break;
            }
        }
    }
}
