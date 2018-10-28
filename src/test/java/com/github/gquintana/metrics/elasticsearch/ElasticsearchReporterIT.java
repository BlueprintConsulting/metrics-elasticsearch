package com.github.gquintana.metrics.elasticsearch;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ElasticsearchReporterIT {
    @Test
    public void testReport() throws MalformedURLException, InterruptedException {
        MetricRegistry registry = new MetricRegistry();
        ElasticsearchReporter reporter = ElasticsearchReporter.forRegistry(registry)
                .hostname("localhost")
                .url("http://localhost:9200")
                .docType("doc")
                .build();
        reporter.start(500, TimeUnit.MILLISECONDS);
        addMetrics(registry);
        Thread.sleep(500L);
        reporter.close();
    }

    private void addMetrics(MetricRegistry registry) throws InterruptedException {
        // JVM and System metrics
        registry.registerAll(new ThreadStatesGaugeSet());
        registry.registerAll(new MemoryUsageGaugeSet());
        registry.registerAll(new GarbageCollectorMetricSet());

        // Application metrics
        Random random = new Random();
        registry.register("gauge", new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return random.nextInt(100);
            }
        });
        Counter counter = registry.counter("counter");
        counter.inc(random.nextInt(100));
        Meter meter = registry.meter("meter");
        for (int i = 0; i < 10; i++) {
            meter.mark();
            Thread.sleep((long) random.nextInt(100));
        }
        Histogram histogram = registry.histogram("histogram");
        for (int i = 0; i < 10; i++) {
            histogram.update(random.nextInt(100));
        }
        Timer timer = registry.timer("timer");
        for (int i = 0; i < 10; i++) {
            timer.update(random.nextInt(1000), TimeUnit.MILLISECONDS);
            Thread.sleep((long) random.nextInt(100));
        }
    }

}
