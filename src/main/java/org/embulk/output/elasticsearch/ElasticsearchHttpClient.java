/*
 * Copyright 2017 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.output.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.spi.JsonProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.HttpHost;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.embulk.config.ConfigException;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.AuthMethod;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.NodeAddressTask;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.PluginTask;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.retryhelper.jetty92.Jetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.embulk.util.retryhelper.jetty92.Jetty92SingleRequester;
import org.embulk.util.retryhelper.jetty92.StringJetty92ResponseEntityReader;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.InfoResponse;
import org.opensearch.client.opensearch.indices.get_alias.IndexAliases;
import org.opensearch.client.opensearch.indices.GetAliasResponse;
import org.opensearch.client.opensearch.indices.GetIndexRequest;
import org.opensearch.client.opensearch.indices.GetIndexResponse;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;

import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.Optional;
import java.util.stream.Collectors;

public class ElasticsearchHttpClient
{
    private final Logger log;

    // ALLOW_UNQUOTED_CONTROL_CHARS - Not expected but whether parser will allow JSON Strings to contain unquoted control characters
    // FAIL_ON_UNKNOWN_PROPERTIES - Feature that determines whether encountering of unknown properties
    private final ObjectMapper jsonMapper = new ObjectMapper()
            .configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false)
            .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // Elasticsearch maximum index byte size
    // public static final int MAX_INDEX_NAME_BYTES = 255;
    // @see https://github.com/elastic/elasticsearch/blob/master/core/src/main/java/org/elasticsearch/cluster/metadata/MetaDataCreateIndexService.java#L108
    private final long maxIndexNameBytes = 255;
    private final List<Character> invalidIndexCharacters = Arrays.asList('\\', '/', '*', '?', '"', '<', '>', '|', '#', ' ', ',');

    public ElasticsearchHttpClient()
    {
        this.log = LoggerFactory.getLogger(getClass());
    }

    public void push(JsonNode records, PluginTask task)
    {
        if (records.size() == 0) {
            return;
        }

        // curl -xPOST localhost:9200/{index}/{type}/_bulk -d '
        // {"index" : {}}\n
        // {"k" : "v"}\n
        // {"index" : {}}\n
        // {"k" : "v2"}\n
        // '
        sendBulkRequest(records, task);
    }

    public List<String> getIndexByAlias(String aliasName, PluginTask task)
    {
        // curl -XGET localhost:9200/_alias/{alias}
        // No alias: 404
        // Alias found: {"embulk_20161018-183738":{"aliases":{"embulk":{}}}}
        GetAliasResponse getAliasResponse = sendGetAliasRequest(aliasName, task);
        Map<String, IndexAliases> result = getAliasResponse.result();
        if (result == null || result.isEmpty()) {
            return new ArrayList<>();
        }

        return result.keySet().stream().collect(Collectors.toList());
    }

    public boolean isIndexExisting(String indexName, PluginTask task)
    {
        // curl -XGET localhost:9200/{index}
        // No index: 404
        // Index found: 200
        try {
            sendGetIndexRequest(indexName, task);
            return true;
        }
        catch (ResourceNotFoundException ex) {
            return false;
        }
    }

    public String generateNewIndexName(String indexName)
    {
        Timestamp time = Exec.getTransactionTime();
        return indexName + new SimpleDateFormat("_yyyyMMdd-HHmmss").format(time.toEpochMilli());
    }

    public boolean isAliasExisting(String aliasName, PluginTask task)
    {
        // curl -XGET localhost:9200/_aliases  // List all aliases
        // No aliases: {}
        // Aliases found: {"embulk_20161018-183738":{"aliases":{"embulk":{}}}}
        JsonNode response = sendRequest("/_aliases", HttpMethod.GET, task);
        if (response.size() == 0) {
            return false;
        }
        for (JsonNode index : response) {
            if (index.has("aliases") && index.get("aliases").has(aliasName)) {
                return true;
            }
        }
        return false;
    }

    // Should be called just once while Embulk transaction.
    // Be sure to call after all exporting tasks completed
    // This method will delete existing index
    public void reassignAlias(String aliasName, String newIndexName, PluginTask task)
    {
        if (!isAliasExisting(aliasName, task)) {
            assignAlias(newIndexName, aliasName, task);
        }
        else {
            List<String> oldIndices = getIndexByAlias(aliasName, task);
            assignAlias(newIndexName, aliasName, task);
            for (String index : oldIndices) {
                deleteIndex(index, task);
            }
        }
    }

    public String getEsVersion(PluginTask task)
    {
        // curl -XGET 'http://localhost:9200'
        return sendInfoRequest(task).version().number();
    }

    public void validateIndexOrAliasName(String index, String type)
    {
        for (int i = 0; i < index.length(); i++) {
            if (invalidIndexCharacters.contains(index.charAt(i))) {
                throw new ConfigException(String.format("%s '%s' must not contain the invalid characters " + invalidIndexCharacters.toString(), type, index));
            }
        }

        if (!index.toLowerCase(Locale.ROOT).equals(index)) {
            throw new ConfigException(String.format("%s '%s' must be lowercase", type, index));
        }

        if (index.startsWith("_") || index.startsWith("-") || index.startsWith("+")) {
            throw new ConfigException(String.format("%s '%s' must not start with '_', '-', or '+'", type, index));
        }

        if (index.length() > maxIndexNameBytes) {
            throw new ConfigException(String.format("%s name is too long, (%s > %s)", type, index.length(), maxIndexNameBytes));
        }

        if (index.equals(".") || index.equals("..")) {
            throw new ConfigException("index must not be '.' or '..'");
        }
    }

    private Optional<String> getRecordId(JsonNode record, Optional<String> idColumn)
    {
        if (idColumn.isPresent() && record.hasNonNull(idColumn.get())) {
            return Optional.of(record.get(idColumn.get()).asText());
        }

        return Optional.empty();
    }

    private void assignAlias(String indexName, String aliasName, PluginTask task)
    {
        try {
            if (isIndexExisting(indexName, task)) {
                if (isAliasExisting(aliasName, task)) {
                    // curl -XPUT http://localhost:9200/_alias -d\
                    // "actions" : [
                    //   {"remove" : {"alias" : "{alias}", "index" : "{index_old}"}},
                    //   {"add" : {"alias": "{alias}", "index": "{index_new}"}}
                    // ]
                    // Success: {"acknowledged":true}
                    List<String> oldIndices = getIndexByAlias(aliasName, task);

                    Map<String, String> newAlias = new HashMap<>();
                    newAlias.put("alias", aliasName);
                    newAlias.put("index", indexName);
                    Map<String, Map> add = new HashMap<>();
                    add.put("add", newAlias);

                    Map<String, String> oldAlias = new HashMap<>();
                    // TODO multiple alias?
                    for (String oldIndex : oldIndices) {
                        oldAlias.put("alias", aliasName);
                        oldAlias.put("index", oldIndex);
                    }
                    Map<String, Map> remove = new HashMap<>();
                    remove.put("remove", oldAlias);

                    List<Map<String, Map>> actions = new ArrayList<>();
                    actions.add(remove);
                    actions.add(add);
                    Map<String, List> rootTree = new HashMap<>();
                    rootTree.put("actions", actions);

                    String content = jsonMapper.writeValueAsString(rootTree);
                    sendRequest("/_aliases", HttpMethod.POST, task, content);
                    log.info("Reassigned alias [{}] to index[{}]", aliasName, indexName);
                }
                else {
                    // curl -XPUT http://localhost:9200/{index}/_alias/{alias}
                    // Success: {"acknowledged":true}
                    String path = String.format("/%s/_alias/%s", indexName, aliasName);
                    sendRequest(path, HttpMethod.PUT, task);
                    log.info("Assigned alias [{}] to Index [{}]", aliasName, indexName);
                }
            }
        }
        catch (JsonProcessingException ex) {
            throw new ConfigException(String.format("Failed to assign alias[%s] to index[%s]", aliasName, indexName));
        }
    }

    private void deleteIndex(String indexName, PluginTask task)
    {
        // curl -XDELETE localhost:9200/{index}
        // Success: {"acknowledged":true}
        if (isIndexExisting(indexName, task)) {
            waitSnapshot(task);
            sendRequest(indexName, HttpMethod.DELETE, task);
            log.info("Deleted Index [{}]", indexName);
        }
    }

    private void waitSnapshot(PluginTask task)
    {
        int maxSnapshotWaitingMills = task.getMaxSnapshotWaitingSecs() * 1000;
        long execCount = 1;
        long totalWaitingTime = 0;
        // Since only needs exponential backoff, don't need exception handling and others, I don't use Embulk RetryExecutor
        while (isSnapshotProgressing(task)) {
            long sleepTime = ((long) Math.pow(2, execCount) * 1000);
            try {
                Thread.sleep(sleepTime);
            }
            catch (InterruptedException ex) {
                // do nothing
            }
            if (execCount > 1) {
                log.info("Waiting for snapshot completed.");
            }
            execCount++;
            totalWaitingTime += sleepTime;
            if (totalWaitingTime > maxSnapshotWaitingMills) {
                throw new ConfigException(String.format("Waiting creating snapshot is expired. %s sec.", maxSnapshotWaitingMills));
            }
        }
    }

    private boolean isSnapshotProgressing(PluginTask task)
    {
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html#_snapshot_status
        // curl -XGET localhost:9200/_snapshot/_status
        JsonNode response = sendRequest("/_snapshot/_status", HttpMethod.GET, task);
        String snapshots = response.get("snapshots").asText();
        return !snapshots.equals("");
    }

    private BulkResponse sendBulkRequest(JsonNode records, PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper2(task)) {
            JsonpMapper jsonpMapper = retryHelper.jsonpMapper();
            JsonProvider jsonProvider = retryHelper.jsonProvider();
            Optional<String> idColumn = task.getId();
            BulkRequest.Builder br = new BulkRequest.Builder();

            for (JsonNode record : records) {
                // TODO: performance
                JsonData jsonData = parseRecord(record, jsonpMapper, jsonProvider);
                Optional<String> id = getRecordId(record, idColumn);

                br.operations(op -> op
                    .index(idx -> idx
                        .index(task.getIndex())
                        .id(id.orElse(null))
                        .document(jsonData)
                    )
                );
            }
            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester() {
                        @Override
                        public <T> T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client, final Class<T> clazz)
                        {
                            try {
                                // TODO: no cast
                                return clazz.cast(client.bulk(br.build()));
                            }
                            catch (IOException e) {
                                // TODO
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        protected boolean isExceptionToRetry(Exception exception)
                        {
                            return task.getId().isPresent();
                        }
                    }, BulkResponse.class);
        }
    }

    private GetAliasResponse sendGetAliasRequest(String aliasName, PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper2(task)) {
            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester() {
                        @Override
                        public <T> T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client, final Class<T> clazz)
                        {
                            try {
                                // TODO: no cast
                                return clazz.cast(client.indices().getAlias(a -> a.name(aliasName)));
                            }
                            catch (IOException e) {
                                // TODO
                                throw new RuntimeException(e);
                            }
                        }
                    }, GetAliasResponse.class);
        }
    }

    private GetIndexResponse sendGetIndexRequest(String indexName, PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper2(task)) {
            GetIndexRequest request = new GetIndexRequest.Builder().index(indexName).build();

            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester() {
                        @Override
                        public <T> T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client, final Class<T> clazz)
                        {
                            try {
                                // TODO: no cast
                                return clazz.cast(client.indices().get(request));
                            }
                            catch (IOException e) {
                                // TODO
                                throw new RuntimeException(e);
                            }
                        }
                    }, GetIndexResponse.class);
        }
    }

    private InfoResponse sendInfoRequest(final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper2(task)) {
            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester() {
                        @Override
                        public <T> T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client, final Class<T> clazz)
                        {
                            try {
                                // TODO: no cast
                                return clazz.cast(client.info());
                            }
                            catch (IOException e) {
                                // TODO
                                throw new RuntimeException(e);
                            }
                        }
                    }, InfoResponse.class);
        }
    }

    private JsonNode sendRequest(String path, final HttpMethod method, PluginTask task)
    {
        return sendRequest(path, method, task, "");
    }

    private JsonNode sendRequest(String path, final HttpMethod method, final PluginTask task, final String content)
    {
        final String uri = createRequestUri(task, path);
        final String authorizationHeader = getAuthorizationHeader(task);

        try (Jetty92RetryHelper retryHelper = createRetryHelper(task)) {
            String responseBody = retryHelper.requestWithRetry(
                    new StringJetty92ResponseEntityReader(task.getTimeoutMills()),
                    new Jetty92SingleRequester() {
                        @Override
                        public void requestOnce(org.eclipse.jetty.client.HttpClient client, org.eclipse.jetty.client.api.Response.Listener responseListener)
                        {
                            org.eclipse.jetty.client.api.Request request = client
                                    .newRequest(uri)
                                    .accept("application/json")
                                    .timeout(task.getTimeoutMills(), TimeUnit.MILLISECONDS)
                                    .method(method);
                            if (method == HttpMethod.POST) {
                                request.content(new StringContentProvider(content), "application/json");
                            }

                            if (!authorizationHeader.isEmpty()) {
                                request.header("Authorization", authorizationHeader);
                            }
                            request.send(responseListener);
                        }

                        @Override
                        protected boolean isExceptionToRetry(Exception exception)
                        {
                            return task.getId().isPresent();
                        }

                        @Override
                        public boolean isResponseStatusToRetry(org.eclipse.jetty.client.api.Response response)
                        {
                            int status = response.getStatus();
                            if (status == 404) {
                                throw new ResourceNotFoundException("Requested resource was not found");
                            }
                            else if (status == 429) {
                                return true;  // Retry if 429.
                            }
                            return status / 100 != 4;  // Retry unless 4xx except for 429.
                        }
                    });
            return parseJson(responseBody);
        }
    }

    private String createRequestUri(PluginTask task, String path)
    {
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        String protocol = task.getUseSsl() ? "https" : "http";
        String nodeAddress = getRandomNodeAddress(task);
        return String.format("%s://%s%s", protocol, nodeAddress, path);
    }

    // Return node address (RoundRobin)
    private String getRandomNodeAddress(PluginTask task)
    {
        List<NodeAddressTask> nodes = task.getNodes();
        Random random = new Random();
        int index = random.nextInt(nodes.size());
        NodeAddressTask node =  nodes.get(index);
        return node.getHost() + ":" + node.getPort();
    }

    private JsonNode parseJson(final String json) throws DataException
    {
        try {
            return this.jsonMapper.readTree(json);
        }
        catch (IOException ex) {
            throw new DataException(ex);
        }
    }

    private OpenSearchRetryHelper createRetryHelper2(final PluginTask task)
    {
        return new OpenSearchRetryHelper(
                task.getMaximumRetries(),
                task.getInitialRetryIntervalMillis(),
                task.getMaximumRetryIntervalMillis(),
                new OpenSearchClientCreator() {
                    @Override
                    public org.opensearch.client.opensearch.OpenSearchClient createAndStart()
                    {
                        RestClient restClient = null;
                        try {
                            // TODO: secret, authorization, timeout
                            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY,
                                new UsernamePasswordCredentials("admin", "admin"));

                            restClient = RestClient.builder(new HttpHost("opensearch", 9200, "http")).
                              setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                @Override
                                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder)
                                {
                                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                }
                              }).build();
                            OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
                            return new OpenSearchClient(transport);
                        }
                        catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                });
    }

    private Jetty92RetryHelper createRetryHelper(final PluginTask task)
    {
        return new Jetty92RetryHelper(
                task.getMaximumRetries(),
                task.getInitialRetryIntervalMillis(),
                task.getMaximumRetryIntervalMillis(),
                new Jetty92ClientCreator() {
                    @Override
                    public org.eclipse.jetty.client.HttpClient createAndStart()
                    {
                        try {
                            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY,
                                new UsernamePasswordCredentials("admin", "admin"));

                            RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "https")).
                              setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                @Override
                                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder)
                                {
                                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                }
                              }).build();
                            OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
                            OpenSearchClient client = new OpenSearchClient(transport);
                        }
                        catch (Exception e) {
                            if (e instanceof RuntimeException) {
                                throw (RuntimeException) e;
                            }
                            throw new RuntimeException(e);
                        }

                        org.eclipse.jetty.client.HttpClient client = new org.eclipse.jetty.client.HttpClient(new SslContextFactory());
                        client.setConnectTimeout(task.getConnectTimeoutMills());
                        try {
                            client.start();
                            return client;
                        }
                        catch (Exception e) {
                            if (e instanceof RuntimeException) {
                                throw (RuntimeException) e;
                            }
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    private JsonData parseRecord(JsonNode record, JsonpMapper jsonpMapper, JsonProvider jsonProvider)
    {
        return JsonData.from(jsonProvider.createParser(new StringReader(record.toString())), jsonpMapper);
    }

    protected String getAuthorizationHeader(PluginTask task)
    {
        String header = "";
        if (task.getAuthMethod() == AuthMethod.BASIC) {
            String authString = task.getUser().get() + ":" + task.getPassword().get();
            header = "Basic " + DatatypeConverter.printBase64Binary(authString.getBytes());
        }
        return header;
    }
}
