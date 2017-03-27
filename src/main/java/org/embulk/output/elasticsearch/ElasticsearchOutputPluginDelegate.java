package org.embulk.output.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.embulk.base.restclient.RestClientOutputPluginDelegate;
import org.embulk.base.restclient.RestClientOutputTaskBase;
import org.embulk.base.restclient.jackson.JacksonServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.jackson.scope.JacksonAllInObjectScope;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.util.retryhelper.jetty92.Jetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;

import java.util.List;
import java.util.Locale;

public class ElasticsearchOutputPluginDelegate
        implements RestClientOutputPluginDelegate<ElasticsearchOutputPluginDelegate.PluginTask>
{
    private final Logger log;
    private final ElasticsearchHttpClient client;

    public ElasticsearchOutputPluginDelegate()
    {
        this.log = Exec.getLogger(getClass());
        this.client = new ElasticsearchHttpClient();
    }

    public interface NodeAddressTask
            extends Task
    {
        @Config("host")
        String getHost();

        @Config("port")
        @ConfigDefault("9200")
        int getPort();
    }

    public interface PluginTask
            extends RestClientOutputTaskBase, TimestampFormatter.Task
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

        @Config("use_ssl")
        @ConfigDefault("false")
        boolean getUseSsl();

        @Config("auth_method")
        @ConfigDefault("\"none\"")
        AuthMethod getAuthMethod();

        @Config("user")
        @ConfigDefault("null")
        Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        Optional<String> getPassword();

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

        @Config("time_zone")
        @ConfigDefault("\"UTC\"")
        String getTimeZone();
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

    public enum AuthMethod
    {
        NONE,
        BASIC;

        @JsonValue
        @Override
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        public static AuthMethod fromString(String value)
        {
            switch (value) {
                case "none":
                    return NONE;
                case "basic":
                    return BASIC;
                default:
                    throw new ConfigException(String.format("Unknown auth_method '%s'. Supported auth_method are none, basic", value));
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
                if (node.getPort() == 9300) {
                    log.warn("Port:9300 is usually used by TransportClient. HTTP/Rest Client uses 9200.");
                }
            }
        }

        try (Jetty92RetryHelper retryHelper = createRetryHelper(task)) {
            log.info(String.format("Connecting to Elasticsearch version:%s", client.getEsVersion(task, retryHelper)));
            log.info("Executing plugin with '{}' mode.", task.getMode());
            client.validateIndexOrAliasName(task.getIndex(), "index");
            client.validateIndexOrAliasName(task.getType(), "index_type");

            if (task.getMode().equals(Mode.REPLACE)) {
                task.setAlias(Optional.of(task.getIndex()));
                task.setIndex(client.generateNewIndexName(task.getIndex()));
                if (client.isIndexExisting(task.getAlias().orNull(), task, retryHelper) && !client.isAliasExisting(task.getAlias().orNull(), task, retryHelper)) {
                    throw new ConfigException(String.format("Invalid alias name [%s], an index exists with the same name as the alias", task.getAlias().orNull()));
                }
            }
            log.info(String.format("Inserting data into index[%s]", task.getIndex()));
        }

        if (task.getAuthMethod() == AuthMethod.BASIC) {
            if (!task.getUser().isPresent() || !task.getPassword().isPresent()) {
                throw new ConfigException("'user' and 'password' are required when auth_method='basic'");
            }
        }
    }

    @Override  // Overridden from |ServiceRequestMapperBuildable|
    public JacksonServiceRequestMapper buildServiceRequestMapper(PluginTask task)
    {
        TimestampFormatter formatter = new TimestampFormatter(task.getJRuby(), "%Y-%m-%dT%H:%M:%S.%3N%z", DateTimeZone.forID(task.getTimeZone()));

        return JacksonServiceRequestMapper.builder()
                .add(new JacksonAllInObjectScope(formatter), new JacksonTopLevelValueLocator("record"))
                .build();
    }

    @Override  // Overridden from |RecordBufferBuildable|
    public RecordBuffer buildRecordBuffer(PluginTask task)
    {
        Jetty92RetryHelper retryHelper = createRetryHelper(task);
        return new ElasticsearchRecordBuffer("records", task, retryHelper);
    }

    @Override
    public ConfigDiff egestEmbulkData(final PluginTask task,
                                      Schema schema,
                                      int taskIndex,
                                      List<TaskReport> taskReports)
    {
        int totalInserted = 0;
        for (TaskReport taskReport : taskReports) {
            if (taskReport.has("inserted")) {
                totalInserted += taskReport.get(int.class, "inserted");
            }
        }

        if (totalInserted > 0) {
            log.info("Insert completed. {} records", totalInserted);
            try (Jetty92RetryHelper retryHelper = createRetryHelper(task)) {
                // Re assign alias only when repale mode
                if (task.getMode().equals(Mode.REPLACE)) {
                    client.reassignAlias(task.getAlias().orNull(), task.getIndex(), task, retryHelper);
                }
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
