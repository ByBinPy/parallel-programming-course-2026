package itmo.ipkn.metrics.domain;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static itmo.ipkn.metrics.services.MetricsCollector.BUCKETS_COUNT;

public record ThreadState(AtomicLongArray buckets, AtomicLong count, AtomicLong sum, AtomicLong min, AtomicLong max) {

    public static ThreadState of() {
        return new ThreadState(new AtomicLongArray(BUCKETS_COUNT), new AtomicLong(), new AtomicLong(), new AtomicLong(Long.MAX_VALUE), new AtomicLong(0));
    }

    public ThreadState clone() {
        long[] bucketsBuff = new long[BUCKETS_COUNT];
        for (int i = 0; i < BUCKETS_COUNT; i++) {
            bucketsBuff[i] = buckets.getPlain(i);
        }
        return new ThreadState(new AtomicLongArray(bucketsBuff), new AtomicLong(count.getPlain()), new AtomicLong(sum.getPlain()), new AtomicLong(min.getPlain()), new AtomicLong(max.getPlain()));
    }
}
