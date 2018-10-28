package com.github.gquintana.metrics.elasticsearch;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Metric which be converted to JSON and written in Elasticsearch
 */
public class MetricSet {
    private final long timestamp;
    private final String hostname;
    private final Map<String, Object> metrics = new HashMap<>();
    private static final DurationConverter NOOP_DURATION_CONVERTER = new DurationConverter() {
        @Override
        public long convert(long duration) {
            return duration;
        }

        @Override
        public double convert(double duration) {
            return duration;
        }
    };

    public MetricSet(long timestamp, String hostname) {
        this.timestamp = timestamp;
        this.hostname = hostname;
    }

    /**
     * All non word characters are forbidden
     */
    private static final Pattern REPLACED_CHARS = Pattern.compile("[^\\w.]+");

    private String normalizeName(String name) {
        return REPLACED_CHARS.matcher(name).replaceAll("_").toLowerCase();
    }

    private Map<String, Object> resolve(String... path) {
        Map<String, Object> current = metrics;
        for (String n : path) {
            String nn = normalizeName(n);
            Object o = current.get(nn);
            if (o instanceof Map) {
                current = (Map) o;
            } else if (o == null) {
                o = new HashMap<>();
                current.put(nn, o);
                current = (Map) o;
            } else {
                throw new IllegalArgumentException("Duplicate key " + nn);
            }
        }
        return current;
    }

    public Map<String, Object> resolve(String path) {
        return resolve(path.split("\\."));
    }

    public void addGauge(String name, Gauge gauge) {
        resolve(name).put("value", gauge.getValue());
    }

    public void addCounter(String name, Counter counter) {
        addCounting(resolve(name), counter);
    }

    public void addHistogram(String name, Histogram histogram) {
        Map<String, Object> metric = resolve(name);
        addCounting(metric, histogram);
        addSampling(metric, histogram, NOOP_DURATION_CONVERTER);
    }

    private void addCounting(Map<String, Object> metric, Counting counting) {
        metric.put("count", counting.getCount());
    }

    private void addSampling(Map<String, Object> metric, Sampling sampling, DurationConverter durationConverter) {
        Snapshot snapshot = sampling.getSnapshot();
        metric.put("min", durationConverter.convert(snapshot.getMin()));
        metric.put("max", durationConverter.convert(snapshot.getMax()));
        metric.put("mean", durationConverter.convert(snapshot.getMean()));
        metric.put("stddev", durationConverter.convert(snapshot.getStdDev()));
        metric.put("median", durationConverter.convert(snapshot.getMedian()));
        metric.put("percentile75", durationConverter.convert(snapshot.get75thPercentile()));
        metric.put("percentile95", durationConverter.convert(snapshot.get95thPercentile()));
        metric.put("percentile99", durationConverter.convert(snapshot.get99thPercentile()));
    }

    public void addMeter(String name, Meter meter, RateConverter rateConverter) {
        Map<String, Object> metric = resolve(name);
        addMetered(metric, meter, rateConverter);
    }

    protected void addMetered(Map<String, Object> metric, Metered metered, RateConverter rateConverter) {
        addCounting(metric, metered);
        metric.put("count", metered.getCount());
        metric.put("rate1m", rateConverter.convert(metered.getOneMinuteRate()));
        metric.put("rate5m", rateConverter.convert(metered.getFiveMinuteRate()));
        metric.put("rate15m", rateConverter.convert(metered.getFifteenMinuteRate()));
        metric.put("ratemean", rateConverter.convert(metered.getMeanRate()));
    }

    public void addTimer(String name, Timer timer, DurationConverter durationConverter, RateConverter rateConverter) {
        Map<String, Object> metric = resolve(name);
        addSampling(metric, timer, durationConverter);
        addMetered(metric, timer, rateConverter);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getHostname() {
        return hostname;
    }

    public Map<String, Object> getMetrics() {
        return metrics;
    }

    public void write(JsonGenerator json) throws IOException {
        json.writeStartObject();
        json.writeNumberField("@timestamp", timestamp);
        // Metric set
        json.writeObjectFieldStart("metricset");
        json.writeStringField("module", "dropwizard");
        json.writeStringField("name", "dropwizard");
        json.writeEndObject();
        // Beat
        json.writeObjectFieldStart("beat");
        json.writeStringField("name", "dropwizard");
        json.writeStringField("hostname", hostname);
        json.writeEndObject();
        // Host
        json.writeObjectFieldStart("host");
        json.writeStringField("name", "hostname");
        json.writeEndObject();
        // Metric
        json.writeObjectFieldStart("dropwizard");
        write(json, metrics);
        json.writeEndObject();
        // End
        json.writeEndObject();
    }

    private void write(JsonGenerator json, Map<String, Object> map) throws IOException {
        for (Map.Entry<String, Object> mapEntry : map.entrySet()) {
            if (mapEntry.getValue() instanceof Map) {
                Map<String, Object> subMap = (Map) mapEntry.getValue();
                json.writeObjectFieldStart(mapEntry.getKey());
                write(json, subMap);
                json.writeEndObject();
            } else if (mapEntry.getValue() instanceof Integer) {
                json.writeNumberField(mapEntry.getKey(), ((Integer) mapEntry.getValue()).intValue());
            } else if (mapEntry.getValue() instanceof Long) {
                json.writeNumberField(mapEntry.getKey(), ((Long) mapEntry.getValue()).longValue());
            } else if (mapEntry.getValue() instanceof Float) {
                json.writeNumberField(mapEntry.getKey(), ((Float) mapEntry.getValue()).floatValue());
            } else if (mapEntry.getValue() instanceof Double) {
                json.writeNumberField(mapEntry.getKey(), ((Double) mapEntry.getValue()).doubleValue());
            }
        }
    }
}
