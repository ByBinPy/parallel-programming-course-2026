package itmo.ipkn.metrics.domain;


import java.util.concurrent.atomic.AtomicInteger;

public record ThreadBuffers(long[][] buckets, long[] count, long[] sum, long[] min, long[] max, AtomicInteger inside) {

    public static ThreadBuffers of() {
        long[][] buckets = new long[2][256];
        long[] count = new long[2];
        long[] sum = new long[2];
        long[] min = {Long.MAX_VALUE, Long.MAX_VALUE};
        long[] max = {0, 0};
        AtomicInteger inside = new AtomicInteger(-1);
        return new ThreadBuffers(buckets, count, sum, min, max, inside);
    }

}
