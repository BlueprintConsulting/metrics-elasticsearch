package com.github.gquintana.metrics.elasticsearch;

public interface DurationConverter {
    long convert(long duration);

    double convert(double duration);
}
