package itmo.ipkn.metrics;

import itmo.ipkn.metrics.domain.Snapshot;
import itmo.ipkn.metrics.services.MetricsCollector;
import itmo.ipkn.metrics.services.impl.EmptyLockMetricsCollector;
import itmo.ipkn.metrics.services.impl.MetricsCollectorImpl;
import itmo.ipkn.metrics.services.impl.SyncMetricsCollector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Math.pow;

public class ZipfLoadTests {

    private static final int TEST_COUNT = 5;
    private static final int WAIT_SECONDS = 5;

    public static void main(String[] args) throws InterruptedException {
        long[] testValues = new long[1 << 20];
        initValuesByZipfLaw(testValues);
        testSynchronizedEmptyLockCollector(testValues);
    }

    private static void testDefaultWithOneThread(long[] testValues) throws InterruptedException {
        final MetricsCollector syncMetricsCollector = new MetricsCollectorImpl();
        double median = measurePoint(syncMetricsCollector, testValues, 1);
        System.out.printf("BaseLine (test with 1 thread): {%f} ops/sec", median);
    }

    private static void testSynchronizedCollector(long[] testValues) throws InterruptedException {
        final MetricsCollector syncMetricsCollector = new SyncMetricsCollector();
        List<Integer> threadCounts = List.of(1, 2, 4, 8, 16);
        for (Integer threadCount : threadCounts) {
            double median = measurePoint(syncMetricsCollector, testValues, threadCount);
            System.out.printf("Test with %d threads: {%f} ops/sec\n", threadCount, median);
        }
    }

    private static void testSynchronizedEmptyLockCollector(long[] testValues) throws InterruptedException {
        final MetricsCollector syncMetricsCollector = new EmptyLockMetricsCollector();
        List<Integer> threadCounts = List.of(1, 2, 4, 8, 16);
        for (Integer threadCount : threadCounts) {
            double median = measurePoint(syncMetricsCollector, testValues, threadCount);
            System.out.printf("Test with %d threads: {%f} ops/sec\n", threadCount, median);
        }
    }

    private static void initValuesByZipfLaw(long[] testValues) {
        final long seed = 67;
        final double kf = 1.15;
        final int maxValue = 1024;

        double[] cdf = new double[maxValue];
        double sum = 0;
        for (int base = 1; base <= maxValue; base++) {
            sum += 1.0 / pow(base, kf);
            cdf[base - 1] = sum;
        }

        for (int i = 0; i < maxValue; i++) {
            cdf[i] /= sum;
        }

        Random random = new Random(seed);
        for (int i = 0; i < testValues.length; i++) {
            int insertionPoint = Arrays.binarySearch(cdf, random.nextDouble());
            if (insertionPoint < 0) {
                insertionPoint = -insertionPoint - 1;
            }
            testValues[i] = insertionPoint + 1;
        }
    }

    private static double measurePoint(MetricsCollector collector, long[] testValues, int threadCount) throws InterruptedException {
        run(collector, testValues, threadCount, WAIT_SECONDS);
        List<Double> results = new ArrayList<>();
        for (int testNumber = 0; testNumber < TEST_COUNT; testNumber++) {
            results.add(run(collector, testValues, threadCount, WAIT_SECONDS));
        }

        Snapshot finishSnapshot = collector.snapshot();
        System.out.printf("All request count: {%d} ", finishSnapshot.count());

        return median(results);
    }

    private static double run(MetricsCollector metricsCollector, long[] testValues, int threadCount, long waitSeconds) throws InterruptedException {
        long[] ops = new long[threadCount];
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] workerPool = new Thread[threadCount];

        for (int threadNumber = 0; threadNumber < threadCount; threadNumber++) {
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

        long startedAt = System.currentTimeMillis();
        start.countDown();
        Thread.sleep(Duration.ofSeconds(waitSeconds));
        stop.set(true);
        long finishedAt = System.currentTimeMillis();

        for (int i = 0; i < threadCount; i++) {
            workerPool[i].join();
        }

        return ((double) Arrays.stream(ops).sum()) /  ((double) (finishedAt - startedAt) / 1000);
    }

    private static double median(List<Double> values) {
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int n = sorted.size();
        return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
    }
}
