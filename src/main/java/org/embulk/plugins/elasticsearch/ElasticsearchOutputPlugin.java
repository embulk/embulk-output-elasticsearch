package org.embulk.plugins.elasticsearch;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.NextConfig;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaVisitor;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.UUID;

public class ElasticsearchOutputPlugin
        implements OutputPlugin
{
    public interface RunnerTask
            extends Task
    {
        @Config("host")
        @ConfigDefault("localhost")
        public String getHost();

        @Config("port")
        @ConfigDefault("9300")
        public int getPort();

        @Config("index_name")
        @ConfigDefault("embulk")
        public String getIndexName();

        @Config("type_name")
        @ConfigDefault("embulk")
        public String getTypeName();

        @Config("id_key")
        public String getIdKey(); //  TODO Optional

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
    public NextConfig transaction(ConfigSource config, Schema schema,
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

        NextConfig nextConfig = Exec.newNextConfig();
        return nextConfig;
    }

    private Client createClient(final RunnerTask task)
    {
        String host = task.getHost();
        int port = task.getPort();
        //  TODO uses Settings
        //  @see http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/client.html
        Client client = new TransportClient() //  ElasticsearchException
                .addTransportAddress(new InetSocketTransportAddress(host, port));
        return client;
    }

    private BulkProcessor createBulkProcessor(final RunnerTask task, final Client client)
    {
        return BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request)
            {
                //  TODO logging
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response)
            {
                //  TODO logging
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure)
            {
                //  TODO error handling
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
        BulkProcessor bulkProcessor = createBulkProcessor(task, client);
        BulkUploader uploader = new BulkUploader(task, client, bulkProcessor);
        uploader.open(schema);
        return uploader;
    }

    static class BulkUploader implements TransactionalPageOutput
    {
        private Client client;
        private BulkProcessor bulkProcessor;

        private final String index;
        private final String type;
        private final String id;

        private PageReader pageReader;
        private BulkRequestBuilder builder;

        BulkUploader(RunnerTask task, Client client, BulkProcessor bulkProcessor)
        {
            this.client = client;
            this.bulkProcessor = bulkProcessor;

            this.index = task.getIndexName();
            this.type = task.getTypeName();
            this.id = task.getIdKey();
        }

        void open(final Schema schema)
        {
            pageReader = new PageReader(schema);
            builder = client.prepareBulk();
        }

        @Override
        public void add(Page page)
        {
            pageReader.setPage(page);

            while (pageReader.nextRecord()) {
                try {
                    final XContentBuilder contextBuilder = XContentFactory.jsonBuilder().startObject(); //  TODO reusable??
                    pageReader.getSchema().visitColumns(new SchemaVisitor() {
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

                    String _id;
                    if (!StringUtils.isEmpty(id)) {
                        _id = id;
                    } else {
                        _id = UUID.randomUUID().toString();
                    }

                    builder.add(client.prepareIndex(index, type, _id).setSource(contextBuilder));
                    //System.out.println("# added index: " + index + ", type: " + type + ", id: " + _id);
                    //BulkResponse bulkResponse = builder.execute().actionGet();

                } catch (IOException e) {
                    Throwables.propagate(e); //  TODO error handling
                }
            }
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
                bulkProcessor.close();
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
