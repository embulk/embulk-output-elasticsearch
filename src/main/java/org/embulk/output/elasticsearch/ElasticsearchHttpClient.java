package org.embulk.output.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.eclipse.jetty.client.HttpResponseException;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.config.ConfigException;
import org.embulk.config.UserDataException;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.AuthMethod;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.NodeAddressTask;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.PluginTask;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.embulk.util.retryhelper.jetty92.Jetty92SingleRequester;
import org.embulk.util.retryhelper.jetty92.StringJetty92ResponseEntityReader;
import org.slf4j.Logger;

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
    private final List<Character> inalidIndexCharaters = Arrays.asList('\\', '/', '*', '?', '"', '<', '>', '|', '#', ' ', ',');
    private final long maxSnapshotWaitingTime = 30 * 60 * 1000; // 30 minutes

    public ElasticsearchHttpClient()
    {
        this.log = Exec.getLogger(getClass());
    }

    public void push(JsonNode records, PluginTask task, Jetty92RetryHelper retryHelper)
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
            String path = String.format("/%s/%s/_bulk", task.getIndex(), task.getType());
            int recordSize = records.size();
            String idColumn = task.getId().orNull();
            if (recordSize > 0) {
                StringBuilder sb = new StringBuilder();
                for (JsonNode record : records) {
                    sb.append(createIndexRequest(idColumn, record));

                    String requestString = jsonMapper.writeValueAsString(record);
                    sb.append("\n")
                            .append(requestString)
                            .append("\n");
                }
                sendRequest(path, HttpMethod.POST, task, retryHelper, sb.toString());
            }
        }
        catch (JsonProcessingException ex) {
            throw new DataException(ex);
        }
    }

    public List<String> getIndexByAlias(String aliasName, PluginTask task, Jetty92RetryHelper retryHelper)
    {
        // curl -XGET localhost:9200/_alias/{alias}
        // No alias: 404
        // Alias found: {"embulk_20161018-183738":{"aliases":{"embulk":{}}}}
        List<String> indices = new ArrayList<>();
        String path = String.format("/_alias/%s", aliasName);
        JsonNode response = sendRequest(path, HttpMethod.GET, task, retryHelper);

        Iterator it = response.fieldNames();
        while (it.hasNext()) {
            indices.add(it.next().toString());
        }

        return indices;
    }

    public boolean isIndexExisting(String indexName, PluginTask task, Jetty92RetryHelper retryHelper)
    {
        // curl -XGET localhost:9200/{index}
        // No index: 404
        // Index found: 200
        try {
            sendRequest(indexName, HttpMethod.GET, task, retryHelper);
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

    public boolean isAliasExisting(String aliasName, PluginTask task, Jetty92RetryHelper retryHelper)
    {
        // curl -XGET localhost:9200/_aliases  // List all aliases
        // No aliases: {}
        // Aliases found: {"embulk_20161018-183738":{"aliases":{"embulk":{}}}}
        JsonNode response = sendRequest("/_aliases", HttpMethod.GET, task, retryHelper);
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
    public void reassignAlias(String aliasName, String newIndexName, PluginTask task, Jetty92RetryHelper retryHelper)
    {
        if (!isAliasExisting(aliasName, task, retryHelper)) {
            assignAlias(newIndexName, aliasName, task, retryHelper);
        }
        else {
            List<String> oldIndices = getIndexByAlias(aliasName, task, retryHelper);
            assignAlias(newIndexName, aliasName, task, retryHelper);
            for (String index : oldIndices) {
                deleteIndex(index, task, retryHelper);
            }
        }
    }

    public String getEsVersion(PluginTask task, Jetty92RetryHelper retryHelper)
    {
        // curl -XGET 'http://localhost:9200’
        JsonNode response = sendRequest("", HttpMethod.GET, task, retryHelper);
        return response.get("version").get("number").asText();
    }

    public void validateIndexOrAliasName(String index, String type)
    {
        for (int i = 0; i < index.length(); i++) {
            if (inalidIndexCharaters.contains(index.charAt(i))) {
                throw new ConfigException(String.format("%s '%s' must not contain the invalid characters " + inalidIndexCharaters.toString(), type, index));
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

    private void assignAlias(String indexName, String aliasName, PluginTask task, Jetty92RetryHelper retryHelper)
    {
        try {
            if (isIndexExisting(indexName, task, retryHelper)) {
                if (isAliasExisting(aliasName, task, retryHelper)) {
                    // curl -XPUT http://localhost:9200/_alias -d\
                    // "actions" : [
                    //   {"remove" : {"alias" : "{alias}", "index" : "{index_old}"}},
                    //   {"add" : {"alias": "{alias}", "index": "{index_new}"}}
                    // ]
                    // Success: {"acknowledged":true}
                    List<String> oldIndices = getIndexByAlias(aliasName, task, retryHelper);

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
                    sendRequest("/_aliases", HttpMethod.POST, task, retryHelper, content);
                    log.info("Reassigned alias [{}] to index[{}]", aliasName, indexName);
                }
                else {
                    // curl -XPUT http://localhost:9200/{index}/_alias/{alias}
                    // Success: {"acknowledged":true}
                    String path = String.format("/%s/_alias/%s", indexName, aliasName);
                    sendRequest(path, HttpMethod.PUT, task, retryHelper);
                    log.info("Assigned alias [{}] to Index [{}]", aliasName, indexName);
                }
            }
        }
        catch (JsonProcessingException ex) {
            throw new ConfigException(String.format("Failed to assign alias[%s] to index[%s]", aliasName, indexName));
        }
    }

    private void deleteIndex(String indexName, PluginTask task, Jetty92RetryHelper retryHelper)
    {
        // curl -XDELETE localhost:9200/{index}
        // Success: {"acknowledged":true}
        if (isIndexExisting(indexName, task, retryHelper)) {
            waitSnapshot(task, retryHelper);
            sendRequest(indexName, HttpMethod.DELETE, task, retryHelper);
            log.info("Deleted Index [{}]", indexName);
        }
    }

    private void waitSnapshot(PluginTask task, Jetty92RetryHelper retryHelper)
    {
        long execCount = 1;
        long totalWaitingTime = 0;
        while (isSnapshotProgressing(task, retryHelper)) {
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
            if (totalWaitingTime > maxSnapshotWaitingTime) {
                throw new ConfigException(String.format("Waiting creating snapshot is expired. %s sec.", maxSnapshotWaitingTime));
            }
        }
    }

    private boolean isSnapshotProgressing(PluginTask task, Jetty92RetryHelper retryHelper)
    {
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html#_snapshot_status
        // curl -XGET localhost:9200/_snapshot/_status
        JsonNode response = sendRequest("/_snapshot/_status", HttpMethod.GET, task, retryHelper);
        String snapshots = response.get("snapshots").asText();
        return !snapshots.equals("");
    }

    private JsonNode sendRequest(String path, final HttpMethod method, PluginTask task, Jetty92RetryHelper retryHelper)
    {
        return sendRequest(path, method, task, retryHelper, "");
    }

    private JsonNode sendRequest(String path, final HttpMethod method, PluginTask task, Jetty92RetryHelper retryHelper, final String content)
    {
        final String uri = createRequestUri(task, path);
        final String authorizationHeader = getAuthorizationHeader(task);

        try {
            String responseBody = retryHelper.requestWithRetry(
                    new StringJetty92ResponseEntityReader(task.getTimeoutMills()),
                    new Jetty92SingleRequester() {
                        @Override
                        public void requestOnce(org.eclipse.jetty.client.HttpClient client, org.eclipse.jetty.client.api.Response.Listener responseListener)
                        {
                            org.eclipse.jetty.client.api.Request request = client
                                    .newRequest(uri)
                                    .accept("application/json")
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
                        public boolean isResponseStatusToRetry(org.eclipse.jetty.client.api.Response response)
                        {
                            int status = response.getStatus();
                            if (status == 429) {
                                return true;  // Retry if 429.
                            }
                            return status / 100 != 4;  // Retry unless 4xx except for 429.
                        }
                    });
            return parseJson(responseBody);
        }
        catch (HttpResponseException ex) {
            if (ex.getMessage().startsWith("Response not 2xx: 404 Not Found")) {
                throw new ResourceNotFoundException(ex);
            }
            throw ex;
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

    @VisibleForTesting
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

        public ResourceNotFoundException(Throwable cause)
        {
            super(cause);
        }
    }
}
