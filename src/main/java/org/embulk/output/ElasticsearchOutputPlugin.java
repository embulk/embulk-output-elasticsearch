package org.embulk.output;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

    public interface RunnerTask
            extends Task
    {
        @Config("nodes")
        public List<NodeAddressTask> getNodes();

        @Config("index_name")
        public String getIndex();

        @Config("index_type")
        public String getIndexType();

        @Config("doc_id_column")
        @ConfigDefault("null")
        public Optional<String> getIdColumn();

        @Config("bulk_actions")
        @ConfigDefault("1000")
        public int getBulkActions();

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
        final RunnerTask task = config.loadConfig(RunnerTask.class);

        try (Client client = createClient(task)) {
        }

        try {
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
                        List<CommitReport> successCommitReports)
    { }

    private Client createClient(final RunnerTask task)
    {
        //  @see http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/client.html
        Settings settings = ImmutableSettings.settingsBuilder()
                .classLoader(Settings.class.getClassLoader())
                .build();
        TransportClient client = new TransportClient(settings);
        List<NodeAddressTask> nodes = task.getNodes();
        for (NodeAddressTask node : nodes) {
            client.addTransportAddress(new InetSocketTransportAddress(node.getHost(), node.getPort()));
        }
        return client;
    }

    private BulkProcessor newBulkProcessor(final RunnerTask task, final Client client)
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
          .setConcurrentRequests(task.getConcurrentRequests())
          .build();
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema,
                                        int processorIndex)
    {
        final RunnerTask task = taskSource.loadTask(RunnerTask.class);

        Client client = createClient(task);
        BulkProcessor bulkProcessor = newBulkProcessor(task, client);
        ElasticsearchPageOutput pageOutput = new ElasticsearchPageOutput(task, client, bulkProcessor);
        pageOutput.open(schema);
        return pageOutput;
    }

    static class ElasticsearchPageOutput implements TransactionalPageOutput
    {
        private Logger log;

        private Client client;
        private BulkProcessor bulkProcessor;

        private PageReader pageReader;

        private final String index;
        private final String indexType;
        private final String docIdColumn;

        ElasticsearchPageOutput(RunnerTask task, Client client, BulkProcessor bulkProcessor)
        {
            this.log = Exec.getLogger(getClass());

            this.client = client;
            this.bulkProcessor = bulkProcessor;

            this.index = task.getIndex();
            this.indexType = task.getIndexType();
            this.docIdColumn = task.getIdColumn().orNull();
        }

        void open(final Schema schema)
        {
            pageReader = new PageReader(schema);
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
                                contextBuilder.field(column.getName(), pageReader.getBoolean(column));
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
                                contextBuilder.field(column.getName(), pageReader.getLong(column));
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
                                contextBuilder.field(column.getName(), pageReader.getDouble(column));
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
                                contextBuilder.field(column.getName(), pageReader.getString(column));
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
                            //  TODO
                        }
                    });

                    contextBuilder.endObject();
                    bulkProcessor.add(newIndexRequest().source(contextBuilder));

                } catch (IOException e) {
                    Throwables.propagate(e); //  TODO error handling
                }
            }
        }

        private IndexRequest newIndexRequest()
        {
            return Requests.indexRequest(index).type(indexType).id(docIdColumn);
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
        public CommitReport commit()
        {
            CommitReport report = Exec.newCommitReport();
            //  TODO
            return report;
        }

    }
}
