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
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.embulk.config.ConfigException;
import org.embulk.config.UserDataException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;

import java.io.IOException;
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

    public static final int ES_SUPPORT_TYPELESS_API_VERSION = 8;
    private static int ES_CURRENT_MAJOR_VERSION = 0;

    public ElasticsearchHttpClient()
    {
        this.log = LoggerFactory.getLogger(getClass());
    }

    public void push(JsonNode records, PluginTask task)
    {
        int bulkActions = task.getBulkActions();
        long bulkSize = task.getBulkSize();
        // curl -xPOST localhost:9200/{index}/{type}/_bulk -d '
        // {"index" : {}}\n
        // {"k" : "v"}\n
        // {"index" : {}}\n
        // {"k" : "v2"}\n
        // '
        try {
            int esMajorVersion = this.getEsMajorVersion(task);
            String path = esMajorVersion >= ES_SUPPORT_TYPELESS_API_VERSION
                ? String.format("/%s/_bulk", task.getIndex())
                : String.format("/%s/%s/_bulk", task.getIndex(), task.getType());
            int recordSize = records.size();
            String idColumn = task.getId().orElse(null);
            if (recordSize > 0) {
                StringBuilder sb = new StringBuilder();
                for (JsonNode record : records) {
                    sb.append(createIndexRequest(idColumn, record));

                    String requestString = jsonMapper.writeValueAsString(record);
                    sb.append("\n")
                            .append(requestString)
                            .append("\n");
                }
                sendRequest(path, HttpMethod.POST, task, sb.toString());
            }
        }
        catch (JsonProcessingException ex) {
            throw new DataException(ex);
        }
    }

    public List<String> getIndexByAlias(String aliasName, PluginTask task)
    {
        // curl -XGET localhost:9200/_alias/{alias}
        // No alias: 404
        // Alias found: {"embulk_20161018-183738":{"aliases":{"embulk":{}}}}
        List<String> indices = new ArrayList<>();
        String path = String.format("/_alias/%s", aliasName);
        JsonNode response = sendRequest(path, HttpMethod.GET, task);

        Iterator it = response.fieldNames();
        while (it.hasNext()) {
            indices.add(it.next().toString());
        }

        return indices;
    }

    public boolean isIndexExisting(String indexName, PluginTask task)
    {
        // curl -XGET localhost:9200/{index}
        // No index: 404
        // Index found: 200
        try {
            sendRequest(indexName, HttpMethod.GET, task);
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
                deleteAlias(index, aliasName, task);
            }
        }
    }

    public String getEsVersion(PluginTask task)
    {
        // curl -XGET 'http://localhost:9200’
        JsonNode response = sendRequest("", HttpMethod.GET, task);
        return response.get("version").get("number").asText();
    }

    public int getEsMajorVersion(PluginTask task)
    {
        try {
            if (ES_CURRENT_MAJOR_VERSION > 0) {
                return ES_CURRENT_MAJOR_VERSION;
            }

            final String esVersion = getEsVersion(task);
            ES_CURRENT_MAJOR_VERSION = Integer.parseInt(esVersion.substring(0, 1));
            return ES_CURRENT_MAJOR_VERSION;
        }
        catch (Exception ex) {
            throw new RuntimeException("Failed to fetch ES version");
        }
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

    private String createIndexRequest(String idColumn, JsonNode record) throws JsonProcessingException
    {
        // index name and type are set at path("/{index}/{type}"). So no need to set
        if (idColumn != null && record.hasNonNull(idColumn)) {
            // {"index" : {"_id" : "v"}}
            Map<String, Map> indexRequest = new HashMap<>();

            Map<String, JsonNode> idRequest = new HashMap<>();
            idRequest.put("_id", record.get(idColumn));

            indexRequest.put("index", idRequest);
            return jsonMapper.writeValueAsString(indexRequest);
        }
        else {
            // {"index" : {}}
            return "{\"index\" : {}}";
        }
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

    private void deleteAlias(String indexName, String aliasName, PluginTask task)
    {
        try {
            if (isIndexExisting(indexName, task)) {
                if (isAliasExisting(aliasName, task)) {
                    Map<String, String> alias = new HashMap<>();
                    alias.put("index", indexName);
                    alias.put("alias", aliasName);

                    Map<String, Map> remove = new HashMap<>();
                    remove.put("remove", alias);

                    List<Map<String, Map>> actions = new ArrayList<>();
                    actions.add(remove);
                    Map<String, List> rootTree = new HashMap<>();
                    rootTree.put("actions", actions);

                    String content = jsonMapper.writeValueAsString(rootTree);
                    sendRequest("/_aliases", HttpMethod.POST, task, content);
                    log.info("Remove alias [{}] to index[{}]", aliasName, indexName);
                }
            }
        }
        catch (JsonProcessingException ex) {
            throw new ConfigException(String.format("Failed to remove alias[%s] to index[%s]", aliasName, indexName));
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
                        public boolean isExceptionToRetry(Exception exception)
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

    protected String getAuthorizationHeader(PluginTask task)
    {
        String header = "";
        if (task.getAuthMethod() == AuthMethod.BASIC) {
            String authString = task.getUser().get() + ":" + task.getPassword().get();
            header = "Basic " + DatatypeConverter.printBase64Binary(authString.getBytes());
        }
        return header;
    }

    public class ResourceNotFoundException extends RuntimeException implements UserDataException
    {
        protected ResourceNotFoundException()
        {
        }

        public ResourceNotFoundException(String message)
        {
            super(message);
        }

        public ResourceNotFoundException(Throwable cause)
        {
            super(cause);
        }
    }
}
