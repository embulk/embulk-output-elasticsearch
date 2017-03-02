package org.embulk.output.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.embulk.base.restclient.RestClientOutputPluginDelegate;
import org.embulk.base.restclient.RestClientOutputTaskBase;
import org.embulk.base.restclient.jackson.JacksonServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonTaskReportRecordBuffer;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.jackson.scope.JacksonAllInObjectScope;
import org.embulk.base.restclient.jackson.scope.JacksonDirectIntegerScope;
import org.embulk.base.restclient.jackson.scope.JacksonDirectStringScope;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.util.retryhelper.jetty92.Jetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.slf4j.Logger;

import java.util.List;
import java.util.Locale;

public class ElasticsearchOutputPluginDelegate
        implements RestClientOutputPluginDelegate<ElasticsearchOutputPluginDelegate.PluginTask>
{
    private final Logger log;
    private final ElasticsearchUtils utils;

    public ElasticsearchOutputPluginDelegate()
    {
        this.log = Exec.getLogger(getClass());
        this.utils = new ElasticsearchUtils();
    }

    public interface NodeAddressTask
            extends RestClientOutputTaskBase
    {
        @Config("host")
        String getHost();

        @Config("port")
        @ConfigDefault("9200")
        int getPort();
    }

    public interface PluginTask
            extends RestClientOutputTaskBase
    {
        @Config("mode")
        @ConfigDefault("\"insert\"")
        Mode getMode();

        @Config("nodes")
        List<NodeAddressTask> getNodes();

        @Config("cluster_name")
        @ConfigDefault("\"elasticsearch\"")
        String getClusterName();

        @Config("index")
        String getIndex();
        void setIndex(String indexName);

        @Config("alias")
        @ConfigDefault("null")
        Optional<String> getAlias();
        void setAlias(Optional<String> aliasName);

        @Config("index_type")
        String getType();

        @Config("id")
        @ConfigDefault("null")
        Optional<String> getId();

        @Config("bulk_actions")
        @ConfigDefault("1000")
        int getBulkActions();

        @Config("bulk_size")
        @ConfigDefault("5242880")
        long getBulkSize();

        @Config("concurrent_requests")
        @ConfigDefault("5")
        int getConcurrentRequests();

        @Config("maximum_retries")
        @ConfigDefault("7")
        int getMaximumRetries();

        @Config("initial_retry_interval_millis")
        @ConfigDefault("1000")
        int getInitialRetryIntervalMillis();

        @Config("maximum_retry_interval_millis")
        @ConfigDefault("120000")
        int getMaximumRetryIntervalMillis();

        @Config("timeout_millis")
        @ConfigDefault("60000")
        int getTimeoutMills();
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

    @Override  // Overridden from |OutputTaskValidatable|
    public void validateOutputTask(PluginTask task, Schema embulkSchema, int taskCount)
    {
        if (task.getNodes().size() > 0) {
            for (NodeAddressTask node : task.getNodes()) {
                if (node.getHost().endsWith("es.amazonaws.com")) {
                    throw new ConfigException("This plugin does't support AWS Elasticsearch Service.");
                }
            }
        }

        try (Jetty92RetryHelper retryHelper = createRetryHelper(task)) {
            log.info(String.format("Connecting to Elasticsearch version:%s", utils.getEsVersion(task, retryHelper)));

            if (task.getMode().equals(Mode.REPLACE)) {
                task.setAlias(Optional.of(task.getIndex()));
                task.setIndex(utils.generateNewIndexName(task.getIndex()));
                if (utils.isExistsIndex(task.getAlias().orNull(), task, retryHelper) && !utils.isExistsAlias(task.getAlias().orNull(), task, retryHelper)) {
                    throw new ConfigException(String.format("Invalid alias name [%s], an index exists with the same name as the alias", task.getAlias().orNull()));
                }
            }
            log.info(String.format("Inserting data into index[%s]", task.getIndex()));
        }
    }

    @Override  // Overridden from |ServiceRequestMapperBuildable|
    public JacksonServiceRequestMapper buildServiceRequestMapper(PluginTask task)
    {
        return JacksonServiceRequestMapper.builder()
                .add(new JacksonAllInObjectScope(), new JacksonTopLevelValueLocator("record"))
                .build();
    }

    @Override  // Overridden from |RecordBufferBuildable|
    public RecordBuffer buildRecordBuffer(PluginTask task)
    {
        return new JacksonTaskReportRecordBuffer("records");
    }

    @Override
    public ConfigDiff egestEmbulkData(final PluginTask task,
                                      Schema schema,
                                      int taskIndex,
                                      List<TaskReport> taskReports)
    {
        ArrayNode records = JsonNodeFactory.instance.arrayNode();
        for (TaskReport taskReport : taskReports) {
            records.addAll(JacksonTaskReportRecordBuffer.resumeFromTaskReport(taskReport, "records"));
        }

        try (Jetty92RetryHelper retryHelper = createRetryHelper(task)) {
            utils.push(records, task, retryHelper);

            // Re assign alias only when repale mode
            if (task.getMode().equals(Mode.REPLACE)) {
                utils.reAssignAlias(task.getAlias().orNull(), task.getIndex(), task, retryHelper);
            }
        }

        return Exec.newConfigDiff();
    }

    @VisibleForTesting
    protected Jetty92RetryHelper createRetryHelper(PluginTask task)
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
                        try {
                            client.start();
                            return client;
                        }
                        catch (Exception e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });
    }
}