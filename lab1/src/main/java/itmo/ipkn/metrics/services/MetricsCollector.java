package itmo.ipkn.metrics.services;

import itmo.ipkn.metrics.domain.Snapshot;

public interface MetricsCollector {
    void record(long value);
    Snapshot snapshot();
}