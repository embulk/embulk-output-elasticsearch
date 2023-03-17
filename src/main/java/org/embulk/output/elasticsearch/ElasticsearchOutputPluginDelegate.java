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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.embulk.base.restclient.RestClientOutputPluginDelegate;
import org.embulk.base.restclient.RestClientOutputTaskBase;
import org.embulk.base.restclient.jackson.JacksonServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;
import org.embulk.util.timestamp.TimestampFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class ElasticsearchOutputPluginDelegate
        implements RestClientOutputPluginDelegate<ElasticsearchOutputPluginDelegate.PluginTask>
{
    private final Logger log;
    private final ElasticsearchHttpClient client;

    public ElasticsearchOutputPluginDelegate()
    {
        this.log = LoggerFactory.getLogger(getClass());
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

        @Config("connect_timeout_millis")
        @ConfigDefault("60000")
        int getConnectTimeoutMills();

        @Config("max_snapshot_waiting_secs")
        @ConfigDefault("1800")
        int getMaxSnapshotWaitingSecs();

        @Config("time_zone")
        @ConfigDefault("\"UTC\"")
        String getTimeZone();

        @Config("fill_null_for_empty_column")
        @ConfigDefault("false")
        boolean getFillNullForEmptyColumn();
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
                if (node.getPort() == 9300) {
                    log.warn("Port:9300 is usually used by TransportClient. HTTP/Rest Client uses 9200.");
                }
            }
        }

        log.info(String.format("Connecting to Elasticsearch version:%s", client.getEsVersion(task)));
        log.info("Executing plugin with '{}' mode.", task.getMode());
        client.validateIndexOrAliasName(task.getIndex(), "index");

        if (task.getMode().equals(Mode.REPLACE)) {
            task.setAlias(Optional.of(task.getIndex()));
            task.setIndex(client.generateNewIndexName(task.getIndex()));
            if (client.isIndexExisting(task.getAlias().orElse(null), task) && !client.isAliasExisting(task.getAlias().orElse(null), task)) {
                throw new ConfigException(String.format("Invalid alias name [%s], an index exists with the same name as the alias", task.getAlias().orElse(null)));
            }
        }
        log.info(String.format("Inserting data into index[%s]", task.getIndex()));

        if (task.getAuthMethod() == AuthMethod.BASIC) {
            if (!task.getUser().isPresent() || !task.getPassword().isPresent()) {
                throw new ConfigException("'user' and 'password' are required when auth_method='basic'");
            }
        }
    }

    @Override  // Overridden from |ServiceRequestMapperBuildable|
    public JacksonServiceRequestMapper buildServiceRequestMapper(PluginTask task)
    {
        final TimestampFormatter formatter = TimestampFormatter
                .builder("%Y-%m-%dT%H:%M:%S.%3N%z", true)
                .setDefaultZoneFromString(task.getTimeZone())
                .build();

        return JacksonServiceRequestMapper.builder()
                .add(new JacksonAllInObjectScope(formatter, task.getFillNullForEmptyColumn()), new JacksonTopLevelValueLocator("record"))
                .build();
    }

    @Override  // Overridden from |RecordBufferBuildable|
    public RecordBuffer buildRecordBuffer(PluginTask task, Schema schema, int taskIndex)
    {
        return new ElasticsearchRecordBuffer("records", task);
    }

    @Override
    public ConfigDiff egestEmbulkData(final PluginTask task,
                                      Schema schema,
                                      int taskIndex,
                                      List<TaskReport> taskReports)
    {
        long totalInserted = 0;
        for (TaskReport taskReport : taskReports) {
            if (taskReport.has("inserted")) {
                totalInserted += taskReport.get(Long.class, "inserted");
            }
        }

        log.info("Insert completed. {} records", totalInserted);
        // Re assign alias only when repale mode
        if (task.getMode().equals(Mode.REPLACE)) {
            client.reassignAlias(task.getAlias().orElse(null), task.getIndex(), task);
        }

        return ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY.newConfigDiff();
    }
}
