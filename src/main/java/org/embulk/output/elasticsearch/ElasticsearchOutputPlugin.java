package org.embulk.output.elasticsearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.InvalidAliasNameException;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

public class ElasticsearchOutputPlugin
        implements OutputPlugin
{
    public interface NodeAddressTask
            extends Task
    {
        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("9300")
        public int getPort();
    }

    public interface PluginTask
            extends Task
    {
        @Config("mode")
        @ConfigDefault("\"insert\"")
        public Mode getMode();

        @Config("nodes")
        public List<NodeAddressTask> getNodes();

        @Config("cluster_name")
        @ConfigDefault("\"elasticsearch\"")
        public String getClusterName();

        @Config("index")
        public String getIndex();
        public void setIndex(String indexName);

        @Config("alias")
        @ConfigDefault("null")
        public Optional<String> getAlias();
        public void setAlias(Optional<String> aliasName);

        @Config("index_type")
        public String getType();

        @Config("id")
        @ConfigDefault("null")
        public Optional<String> getId();

        @Config("bulk_actions")
        @ConfigDefault("1000")
        public int getBulkActions();

        @Config("bulk_size")
        @ConfigDefault("5242880")
        public long getBulkSize();

        @Config("concurrent_requests")
        @ConfigDefault("5")
        public int getConcurrentRequests();
    }

    private final Logger log;

    @Inject
    public ElasticsearchOutputPlugin()
    {
        log = Exec.getLogger(getClass());
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, Schema schema,
                                  int processorCount, Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);

        // confirm that a client can be initialized
        try (Client client = createClient(task)) {
            log.info(String.format("Executing plugin with '%s' mode.", task.getMode()));
            if (task.getMode().equals(Mode.REPLACE)) {
                task.setAlias(Optional.of(task.getIndex()));
                task.setIndex(generateNewIndexName(task.getIndex()));
                if (isExistsIndex(task.getAlias().orNull(), client) && !isAlias(task.getAlias().orNull(), client)) {
                    throw new ConfigException(String.format("Invalid alias name [%s], an index exists with the same name as the alias", task.getAlias().orNull()));
                }
            }
            log.info(String.format("Inserting data into index[%s]", task.getIndex()));
            control.run(task.dump());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        ConfigDiff nextConfig = Exec.newConfigDiff();
        return nextConfig;
    }

    @Override
    public  ConfigDiff resume(TaskSource taskSource,
                             Schema schema, int processorCount,
                             OutputPlugin.Control control)
    {
        //  TODO
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        Schema schema, int processorCount,
                        List<TaskReport> successTaskReports)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        if (task.getMode().equals(Mode.REPLACE)) {
            try (Client client = createClient(task)) {
                reAssignAlias(task.getAlias().orNull(), task.getIndex(), client);
            } catch (IndexNotFoundException | InvalidAliasNameException e) {
                throw new ConfigException(e);
            }
        }
    }

    private Client createClient(final PluginTask task)
    {
        //  @see http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/client.html
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", task.getClusterName())
                .build();
        TransportClient client = TransportClient.builder().settings(settings).build();
        List<NodeAddressTask> nodes = task.getNodes();
        for (NodeAddressTask node : nodes) {
            try {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(node.getHost()), node.getPort()));
            } catch (UnknownHostException e) {
                Throwables.propagate(e);
            }
        }
        return client;
    }

    private BulkProcessor newBulkProcessor(final PluginTask task, final Client client)
    {
        return BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request)
            {
                log.info("Execute {} bulk actions", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response)
            {
                if (response.hasFailures()) {
                    long items = 0;
                    if (log.isDebugEnabled()) {
                        for (BulkItemResponse item : response.getItems()) {
                            if (item.isFailed()) {
                                items += 1;
                                log.debug("   Error for {}/{}/{} for {} operation: {}",
                                        item.getIndex(), item.getType(), item.getId(),
                                        item.getOpType(), item.getFailureMessage());
                            }
                        }
                    }
                    log.warn("{} bulk actions failed: {}", items, response.buildFailureMessage());
                } else {
                    log.info("{} bulk actions succeeded", request.numberOfActions());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure)
            {
                log.warn("Got the error during bulk processing", failure);
            }
        }).setBulkActions(task.getBulkActions())
          .setBulkSize(new ByteSizeValue(task.getBulkSize()))
          .setConcurrentRequests(task.getConcurrentRequests())
          .build();
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema,
                                        int processorIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        Client client = createClient(task);
        BulkProcessor bulkProcessor = newBulkProcessor(task, client);
        ElasticsearchPageOutput pageOutput = new ElasticsearchPageOutput(task, client, bulkProcessor);
        pageOutput.open(schema);
        return pageOutput;
    }

    public static class ElasticsearchPageOutput implements TransactionalPageOutput
    {
        private Logger log;

        private Client client;
        private BulkProcessor bulkProcessor;

        private PageReader pageReader;
        private Column idColumn;

        private final String index;
        private final String type;
        private final String id;

        public ElasticsearchPageOutput(PluginTask task, Client client, BulkProcessor bulkProcessor)
        {
            this.log = Exec.getLogger(getClass());

            this.client = client;
            this.bulkProcessor = bulkProcessor;

            this.index = task.getIndex();
            this.type = task.getType();
            this.id = task.getId().orNull();
        }

        void open(final Schema schema)
        {
            pageReader = new PageReader(schema);
            idColumn = (id == null) ? null : schema.lookupColumn(id);
        }

        @Override
        public void add(Page page)
        {
            pageReader.setPage(page);

            while (pageReader.nextRecord()) {
                try {
                    final XContentBuilder contextBuilder = XContentFactory.jsonBuilder().startObject(); //  TODO reusable??
                    pageReader.getSchema().visitColumns(new ColumnVisitor() {
                        @Override
                        public void booleanColumn(Column column) {
                            try {
                                if (pageReader.isNull(column)) {
                                    contextBuilder.nullField(column.getName());
                                } else {
                                    contextBuilder.field(column.getName(), pageReader.getBoolean(column));
                                }
                            } catch (IOException e) {
                                try {
                                    contextBuilder.nullField(column.getName());
                                } catch (IOException ex) {
                                    throw Throwables.propagate(ex);
                                }
                            }
                        }

                        @Override
                        public void longColumn(Column column) {
                            try {
                                if (pageReader.isNull(column)) {
                                    contextBuilder.nullField(column.getName());
                                } else {
                                    contextBuilder.field(column.getName(), pageReader.getLong(column));
                                }
                            } catch (IOException e) {
                                try {
                                    contextBuilder.nullField(column.getName());
                                } catch (IOException ex) {
                                    throw Throwables.propagate(ex);
                                }
                            }
                        }

                        @Override
                        public void doubleColumn(Column column) {
                            try {
                                if (pageReader.isNull(column)) {
                                    contextBuilder.nullField(column.getName());
                                } else {
                                    contextBuilder.field(column.getName(), pageReader.getDouble(column));
                                }
                            } catch (IOException e) {
                                try {
                                    contextBuilder.nullField(column.getName());
                                } catch (IOException ex) {
                                    throw Throwables.propagate(ex);
                                }
                            }
                        }

                        @Override
                        public void stringColumn(Column column) {
                            try {
                                if (pageReader.isNull(column)) {
                                    contextBuilder.nullField(column.getName());
                                } else {
                                    contextBuilder.field(column.getName(), pageReader.getString(column));
                                }
                            } catch (IOException e) {
                                try {
                                    contextBuilder.nullField(column.getName());
                                } catch (IOException ex) {
                                    throw Throwables.propagate(ex);
                                }
                            }
                        }

                        @Override
                        public void timestampColumn(Column column) {
                            try {
                                if (pageReader.isNull(column)) {
                                    contextBuilder.nullField(column.getName());
                                } else {
                                    contextBuilder.field(column.getName(), new Date(pageReader.getTimestamp(column).toEpochMilli()));
                                }
                            } catch (IOException e) {
                                try {
                                    contextBuilder.nullField(column.getName());
                                } catch (IOException ex) {
                                    throw Throwables.propagate(ex);
                                }
                            }
                        }
                    });

                    contextBuilder.endObject();
                    bulkProcessor.add(newIndexRequest(getIdValue(idColumn)).source(contextBuilder));

                } catch (IOException e) {
                    Throwables.propagate(e); //  TODO error handling
                }
            }
        }

        /**
         * @param inputColumn
         * @return
         */
        private String getIdValue(Column inputColumn) {
            if (inputColumn == null) return null;
            if (pageReader.isNull(inputColumn)) return null;
            String idValue = null;
            if (Types.STRING.equals(inputColumn.getType())) {
                idValue = pageReader.getString(inputColumn);
            } else if (Types.BOOLEAN.equals(inputColumn.getType())) {
                idValue = pageReader.getBoolean(inputColumn) + "";
            } else if (Types.DOUBLE.equals(inputColumn.getType())) {
                idValue = pageReader.getDouble(inputColumn) + "";
            } else if (Types.LONG.equals(inputColumn.getType())) {
                idValue = pageReader.getLong(inputColumn) + "";
            } else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
                idValue = pageReader.getTimestamp(inputColumn).toString();
            } else {
                idValue = null;
            }
            return idValue;
        }

        private IndexRequest newIndexRequest(String idValue)
        {
            return Requests.indexRequest(index).type(type).id(idValue);
        }

        @Override
        public void finish()
        {
            try {
                bulkProcessor.flush();
            } finally {
                close();
            }
        }

        @Override
        public void close()
        {
            if (bulkProcessor != null) {
                try {
                    while (!bulkProcessor.awaitClose(3, TimeUnit.SECONDS)) {
                        log.debug("wait for closing the bulk processing..");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                bulkProcessor = null;
            }

            if (client != null) {
                client.close(); //  ElasticsearchException
                client = null;
            }
        }

        @Override
        public void abort()
        {
            //  TODO do nothing
        }

        @Override
        public TaskReport commit()
        {
            TaskReport report = Exec.newTaskReport();
            //  TODO
            return report;
        }

    }

    public enum Mode
    {
        INSERT,
        REPLACE;

        @JsonValue
        @Override
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        public static Mode fromString(String value)
        {
            switch (value) {
                case "insert":
                    return INSERT;
                case "replace":
                    return REPLACE;
                default:
                    throw new ConfigException(String.format("Unknown mode '%s'. Supported modes are insert, truncate_insert, replace", value));
            }
        }
    }

    private void reAssignAlias(String aliasName, String newIndexName, Client client)
            throws IndexNotFoundException, InvalidAliasNameException
    {
        if (!isExistsAlias(aliasName, client)) {
            client.admin().indices().prepareAliases()
                    .addAlias(newIndexName, aliasName)
                    .execute().actionGet();
            log.info(String.format("Assigned alias[%s] to index[%s]", aliasName, newIndexName));
        } else {
            List<String> oldIndices = getIndexByAlias(aliasName, client);
            client.admin().indices().prepareAliases()
                    .removeAlias(oldIndices.toArray(new String[oldIndices.size()]), aliasName)
                    .addAlias(newIndexName, aliasName)
                    .execute().actionGet();
            log.info(String.format("Reassigned alias[%s] from index%s to index[%s]", aliasName, oldIndices, newIndexName));
            for (String index : oldIndices) {
                deleteIndex(index, client);
            }
        }
    }

    private void deleteIndex(String indexName, Client client)
    {
        client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
        log.info(String.format("Deleted Index [%s]", indexName));
    }

    private List<String> getIndexByAlias(String aliasName, Client client)
    {
        ImmutableOpenMap<String, List<AliasMetaData>> map = client.admin().indices().getAliases(new GetAliasesRequest(aliasName))
                .actionGet().getAliases();
        List<String> indices = new ArrayList<>();
        for (ObjectObjectCursor<String, List<AliasMetaData>> c : map) {
            indices.add(c.key);
        }

        return indices;
    }

    private boolean isExistsAlias(String aliasName, Client client)
    {
        return client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().getMetaData().hasAlias(aliasName);
    }

    private boolean isExistsIndex(String indexName, Client client)
    {
        return client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().getMetaData().hasIndex(indexName);
    }

    private boolean isAlias(String aliasName, Client client)
    {
        AliasOrIndex aliasOrIndex = client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().getMetaData().getAliasAndIndexLookup().get(aliasName);
        return aliasOrIndex != null && aliasOrIndex.isAlias();
    }

    public String generateNewIndexName(String indexName)
    {
        Timestamp time = Exec.getTransactionTime();
        return indexName + new SimpleDateFormat("_yyyyMMdd-HHmmss").format(time.toEpochMilli());
    }
}
