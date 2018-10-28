package com.github.gquintana.metrics.elasticsearch;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.util.Base64;
import java.util.Date;

public class ElasticsearchClient {
    private final String baseUrl;
    private final String authorization;
    private final String indexPrefix;
    private final DateFormat indexDateFormat;
    private final String docType;
    private final JsonFactory jsonFactory = new JsonFactory();

    /**
     * @param baseUrl         Elasticsearch base URL, ex: http://localhost:9200
     * @param indexPrefix     Index name prefix, ex: dropwizard-
     * @param indexDateFormat Index name date format, ex: yyyy.MM.dd
     * @param docType         Document type (null allowed), ex: doc
     */
    public ElasticsearchClient(String baseUrl, String username, String password, String indexPrefix, DateFormat indexDateFormat, String docType) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        this.authorization = buildAuthorization(username, password);
        this.indexPrefix = indexPrefix.endsWith("-") ? indexPrefix : indexPrefix + "-";
        this.indexDateFormat = indexDateFormat;
        this.docType = docType;
    }

    private static String buildAuthorization(String username, String password) {
        if (username == null || username.isEmpty() || password == null || password.isEmpty()) {
            return null;
        }
        Base64.Encoder base64 = Base64.getEncoder();
        return "Basic " + new String(base64.encode((username + ":" + password).getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    public void postDocument(MetricSet metricSet) {
        HttpURLConnection connection = null;
        try {
            connection = openConnection(metricSet.getTimestamp());
            connection.setRequestMethod("POST");
            if (authorization != null) {
                connection.setRequestProperty("Authorization", authorization);
            }
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
            connection.setDoOutput(true);
            try (OutputStream output = connection.getOutputStream();
                 JsonGenerator jsonGenerator = jsonFactory.createGenerator(output, JsonEncoding.UTF8)) {
                metricSet.write(jsonGenerator);
            }
            if (connection.getResponseCode() >= 300) {
                throw new ElasticsearchException(String.format("Elasticsearch reponse failed,  code %d, message %s", connection.getResponseCode(), connection.getResponseMessage()));
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Elasticsearch connection failed", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    public HttpURLConnection openConnection(long timestamp) throws IOException {
        String url = baseUrl + indexPrefix + indexDateFormat.format(new Date(timestamp));
        if (docType != null) {
            url += "/" + docType;
        }
        return (HttpURLConnection) new URL(url).openConnection();
    }
}
