package com.github.gquintana.metrics.elasticsearch;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ElasticsearchReporter extends ScheduledReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchReporter.class);

    private static final String NAME = "elasticsearch-reporter";
    /**
     * Current host name
     */
    private final String hostname;
    private final ElasticsearchClient elasticsearchClient;

    private final RateConverter rateConverter = new RateConverter() {
        @Override
        public double convert(double rate) {
            return convertRate(rate);
        }
    };

    private final DurationConverter durationConverter = new DurationConverter() {
        @Override
        public long convert(long duration) {
            return (long) convertDuration(duration);
        }

        @Override
        public double convert(double rate) {
            return convertDuration(rate);
        }
    };

    private ElasticsearchReporter(MetricRegistry registry, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, String hostname, ElasticsearchClient elasticsearchClient) {
        super(registry, NAME, filter, rateUnit, durationUnit);
        this.hostname = hostname;
        this.elasticsearchClient = elasticsearchClient;
    }

    private ElasticsearchReporter(MetricRegistry registry, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, ScheduledExecutorService executor, String hostname, ElasticsearchClient elasticsearchClient) {
        super(registry, NAME, filter, rateUnit, durationUnit, executor);
        this.hostname = hostname;
        this.elasticsearchClient = elasticsearchClient;
    }

    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        MetricSet metricSet = new MetricSet(System.currentTimeMillis(), hostname);

        for (Map.Entry<String, Gauge> gauge : gauges.entrySet()) {
            metricSet.addGauge(gauge.getKey(), gauge.getValue());
        }
        for (Map.Entry<String, Counter> counter : counters.entrySet()) {
            metricSet.addCounter(counter.getKey(), counter.getValue());
        }
        for (Map.Entry<String, Histogram> histogram : histograms.entrySet()) {
            metricSet.addHistogram(histogram.getKey(), histogram.getValue());
        }
        for (Map.Entry<String, Meter> meter : meters.entrySet()) {
            metricSet.addMeter(meter.getKey(), meter.getValue(), rateConverter);
        }
        for (Map.Entry<String, Timer> timer : timers.entrySet()) {
            metricSet.addTimer(timer.getKey(), timer.getValue(), durationConverter, rateConverter);
        }
        try {
            elasticsearchClient.postDocument(metricSet);
        } catch (ElasticsearchException e) {
            LOGGER.warn("Failed to write metrics in Elasticsearch", e);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Builder of {@link ElasticsearchReporter}
     */
    public static class Builder {
        private final MetricRegistry registry;
        private TimeUnit rateUnit = TimeUnit.SECONDS;
        private TimeUnit durationUnit = TimeUnit.MILLISECONDS;
        private MetricFilter filter = MetricFilter.ALL;
        private String hostname;
        private String url = "http://localhost:9200/";
        private String username;
        private String password;
        private String indexPrefix = "metricbeat-dropwizard-";
        private DateFormat indexDateFormat = new SimpleDateFormat("yyyy.MM.dd");
        private String docType;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            try {
                this.hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                this.hostname = "unknown";
            }
        }

        /**
         * Rate unit in exported metrics, default: events/second
         */
        public ElasticsearchReporter.Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }


        /**
         * Duration unit in exported metrics, default: milliseconds
         */
        public ElasticsearchReporter.Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Select which metrics must be reported, default: all metrics
         */
        public ElasticsearchReporter.Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Current hostname to be added in metric documents, default: host name
         */
        public ElasticsearchReporter.Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        /**
         * Elasticsearch username and password
         */
        public ElasticsearchReporter.Builder basicAuth(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        /**
         * Elasticsearch URL, ex: https://elasticsearch:9200, default: http://elasticsearch:9200
         */
        public ElasticsearchReporter.Builder url(String url) throws MalformedURLException {
            // Check URL is valid
            new URL(url);
            this.url = url;
            return this;
        }

        /**
         * Index name prefix, ex: metrics-, default: metricbeat-dropwizard-
         */
        public ElasticsearchReporter.Builder indexPrefix(String indexPrefix) {
            this.indexPrefix = indexPrefix;
            return this;
        }

        /**
         * Index name date suffix format, ex: yyyy.MM, default: yyyy.MM.dd
         */
        public ElasticsearchReporter.Builder indexDateFormat(String indexDateFormat) {
            this.indexDateFormat = new SimpleDateFormat(indexDateFormat);
            return this;
        }

        /**
         * Document type, ex: doc
         */
        public ElasticsearchReporter.Builder docType(String docType) {
            this.docType = docType;
            return this;
        }

        public ElasticsearchReporter build() {
            ElasticsearchClient elasticsearchClient = new ElasticsearchClient(url, username, password, indexPrefix, indexDateFormat, docType);
            return new ElasticsearchReporter(this.registry, filter, rateUnit, durationUnit, hostname, elasticsearchClient);
        }
    }

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }
}
