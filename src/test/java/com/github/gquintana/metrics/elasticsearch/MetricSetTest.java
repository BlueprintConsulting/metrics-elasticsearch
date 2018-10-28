package com.github.gquintana.metrics.elasticsearch;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class MetricSetTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private final DurationConverter durationConverter = new DurationConverter() {
        @Override
        public long convert(long duration) {
            return TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS);
        }

        @Override
        public double convert(double duration) {
            return convert((long) duration);
        }
    };
    private final RateConverter rateConverter = new RateConverter() {
        @Override
        public double convert(double rate) {
            return rate;
        }
    };

    @Test
    public void testGauge() throws IOException {
        MetricRegistry registry = new MetricRegistry();
        Gauge<Integer> gauge = new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return 123;
            }
        };
        registry.register("gauge", gauge);
        MetricSet metricSet = new MetricSet(System.currentTimeMillis(), "localhost");
        metricSet.addGauge("gauge", gauge);
        assertThat(get(metricSet, Integer.class, "gauge", "value"), equalTo(123));
        assertThat(metricSet.getHostname(), equalTo("localhost"));
        writeJson(metricSet, "gauge.json");
    }

    @Test
    public void testCounter() throws IOException {
        MetricRegistry registry = new MetricRegistry();
        Counter counter = registry.counter("counter");
        counter.inc();
        counter.inc(2L);
        counter.inc(3L);
        MetricSet metricSet = new MetricSet(System.currentTimeMillis(), "localhost");
        metricSet.addCounter("counter", counter);
        assertThat(get(metricSet, Long.class, "counter", "count"), equalTo(6L));
        writeJson(metricSet, "counter.json");
    }

    private static class DeltaClock extends Clock {
        private final Clock realClock;
        private long delta;

        public DeltaClock(Clock realClock, long delta) {
            this.realClock = realClock;
            this.delta = delta;
        }

        @Override
        public long getTick() {
            return Clock.defaultClock().getTick() - delta;
        }

        @Override
        public long getTime() {
            return Clock.defaultClock().getTime() - delta;
        }

    }

    @Test
    public void testMeter() throws IOException {
        MetricRegistry registry = new MetricRegistry();
        // Put clock in the past
        DeltaClock clock = new DeltaClock(Clock.defaultClock(), TimeUnit.SECONDS.toNanos(5L) + 100L);
        Meter meter = new Meter(clock);
        registry.register("meter", meter);
        meter.mark();
        meter.mark();
        meter.mark(2L);
        // Put clock in the present
        clock.delta = 0L;
        MetricSet metricSet = new MetricSet(System.currentTimeMillis(), "localhost");
        metricSet.addMeter("meter", meter, rateConverter);
        assertThat(get(metricSet, Long.class, "meter", "count"), equalTo(4L));
        assertThat(get(metricSet, Double.class, "meter", "rate1m"), equalTo(0.8D));
        assertThat(get(metricSet, Double.class,"meter", "rate5m"), equalTo(0.8D));
        assertThat(get(metricSet, Double.class,"meter", "rate15m"), equalTo(0.8D));
        writeJson(metricSet, "meter.json");
    }

    @Test
    public void testHistogram() throws IOException {
        MetricRegistry registry = new MetricRegistry();
        Histogram histogram = registry.histogram("histogram");
        histogram.update(1);
        histogram.update(2);
        histogram.update(2);
        histogram.update(3);
        histogram.update(3);
        histogram.update(3);
        histogram.update(4);
        histogram.update(4);
        histogram.update(5);
        MetricSet metricSet = new MetricSet(System.currentTimeMillis(), "localhost");
        metricSet.addHistogram("histogram", histogram);
        assertThat(get(metricSet, Long.class, "histogram", "count"), equalTo(9L));
        assertThat(get(metricSet, Long.class, "histogram", "min"), equalTo(1L));
        assertThat(get(metricSet, Long.class, "histogram", "max"), equalTo(5L));
        assertThat(get(metricSet, Double.class,"histogram", "mean"), equalTo(3D));
        assertThat(get(metricSet, Double.class,"histogram", "median"), equalTo(3D));
        assertThat(get(metricSet, Double.class,"histogram", "percentile75"), equalTo(4D));
        assertThat(get(metricSet, Double.class,"histogram", "percentile95"), equalTo(5D));
        assertThat(get(metricSet, Double.class,"histogram", "percentile99"), equalTo(5D));
        writeJson(metricSet, "histogram.json");
    }

    @Test
    public void testTimer() throws IOException {
        MetricRegistry registry = new MetricRegistry();
        Timer timer = registry.timer("timer");
        timer.update(8L, TimeUnit.SECONDS);
        timer.update(10L, TimeUnit.SECONDS);
        timer.update(14L, TimeUnit.SECONDS);
        MetricSet metricSet = new MetricSet(System.currentTimeMillis(), "localhost");
        metricSet.addTimer("timer", timer, durationConverter, rateConverter);
        assertThat(get(metricSet, Long.class, "timer", "count"), equalTo(3L));
        assertThat(get(metricSet, Long.class, "timer", "min"), equalTo(8000L));
        assertThat(get(metricSet, Long.class, "timer", "max"), equalTo(14000L));
        assertThat(get(metricSet, Double.class, "timer", "mean"), equalTo(10666D));
        writeJson(metricSet, "timer.json");
    }

    private <T> T get(MetricSet metricSet, Class<T> type, String... path) {
        Map<String, Object> map = metricSet.getMetrics();
        for (int i = 0; i < path.length - 1; i++) {
            map = (Map) map.get(path[i]);
        }
        return type.cast(map.get(path[path.length - 1]));
    }

    private void writeJson(MetricSet metricSet, String fileName) throws IOException {
        JsonFactory jsonFactory = new JsonFactory();
        File file = temporaryFolder.newFile(fileName);
        try(JsonGenerator generator = jsonFactory.createGenerator(file, JsonEncoding.UTF8)) {
            metricSet.write(generator);
        }
        try(Scanner scanner = new Scanner(file, "UTF-8")) {
            System.out.println(scanner.next());
        }
    }
}
