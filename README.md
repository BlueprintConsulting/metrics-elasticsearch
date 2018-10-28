# Metrics Elasticsearch Reporter
[Dropwizard Metrics](https://metrics.dropwizard.io) Elasticsearch reporter

This a Yammer|Codahale|Dropwizard Metrics extension to report metrics in an Elasticsearch serveur using Metricbeat-like documents.

Compared to [elasticsearch-metrics-reporter-java](https://github.com/elastic/elasticsearch-metrics-reporter-java):
* Supports Elasticsearch 6.x
* One document with all metrics instead of One document per metric (like Metricbeat)
* Doesn't configure Index template yet

Usage:
```java
MetricRegistry registry = new MetricRegistry();
ElasticsearchReporter reporter = ElasticsearchReporter.forRegistry(registry)
    .url("http://eshost:9200")
    .docType("doc")
    .hostname("myhost")
    .build();
reporter.start(30, TimeUnit.SECONDS);
```

