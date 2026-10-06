package itmo.ipkn.metrics;

import itmo.ipkn.metrics.domain.Snapshot;
import itmo.ipkn.metrics.services.MetricsCollector;
import itmo.ipkn.metrics.services.impl.BufferedMetricsCollector;
import itmo.ipkn.metrics.services.impl.LocalizedMetricsCollector;
import itmo.ipkn.metrics.services.impl.ShardedMetricsCollectorImpl;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static itmo.ipkn.metrics.DataUtil.initValuesByZipfLaw;

public class InconsistencyTest {

    private static final int THREAD_COUNT = 4;
    private static final int CHECK_COUNT = 10_000;


    public static void main(String[] args) throws InterruptedException {
        long[] testValues = new long[1 << 20];
        initValuesByZipfLaw(testValues);
        testInconsistencyForBuffered(testValues);
}

    private static void testInconsistencyForShardedLock(long[] testValues) throws InterruptedException {
        checkInconsistencyStates(testValues, new ShardedMetricsCollectorImpl());
    }

    private static void testInconsistencyForLocalized(long[] testValues) throws InterruptedException {
        checkInconsistencyStates(testValues, new LocalizedMetricsCollector());
    }

    private static void testInconsistencyForBuffered(long[] testValues) throws InterruptedException {
        checkInconsistencyStates(testValues, new BufferedMetricsCollector());
    }

    private static void checkInconsistencyStates(long[] testValues, MetricsCollector metricsCollector) throws InterruptedException {
        long[] ops = new long[THREAD_COUNT];
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] workerPool = new Thread[THREAD_COUNT];

        for (int threadNumber = 0; threadNumber < THREAD_COUNT; threadNumber++) {
            final int finalThreadNumber = threadNumber;
            workerPool[threadNumber] = new Thread(() -> {
                long localCount = 0;
                int startPosition = finalThreadNumber * 1000;
                try {
                    start.await();
                    while (!stop.get()) {
                        metricsCollector.record(testValues[startPosition]);
                        localCount++;
                        startPosition++;
                        if (startPosition == testValues.length) {
                            startPosition = 0;
                        }
                    }
                    ops[finalThreadNumber] = localCount;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            workerPool[threadNumber].start();
        }

        start.countDown();

        long brokenSnapshotCount = 0;
        long lessCount = 0;
        long moreCount = 0;
        for (int i = 0; i < CHECK_COUNT; i++) {
            Snapshot checkSnapshot = metricsCollector.snapshot();
            long sumBuckets = Arrays.stream(checkSnapshot.buckets()).sum();
            if (sumBuckets != checkSnapshot.count()) {
                brokenSnapshotCount++;
                if (sumBuckets > checkSnapshot.count()) {
                    moreCount++;
                } else {
                    lessCount++;
                }
            }
        }
        stop.set(true);

        for (int i = 0; i < THREAD_COUNT; i++) {
            workerPool[i].join();
        }

        System.out.printf("Broken snapshots: {%d}. Buckets sum was less than count: {%d}. Buckets sum was more than count: {%d}\n", brokenSnapshotCount, lessCount, moreCount);
        System.out.printf("MetricsCollector result count: {%d}. Threads result count: {%d}", metricsCollector.snapshot().count(), Arrays.stream(ops).sum());
    }
}
