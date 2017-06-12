package org.embulk.output.elasticsearch;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.base.Throwables;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.config.TaskReport;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * ElasticsearchRecordBuffer is an implementation of {@code RecordBuffer} which includes JSON output directly to Elasticsearch server.
 */
public class ElasticsearchRecordBuffer
        extends RecordBuffer
{
    private final String attributeName;
    private final PluginTask task;
    private final long bulkActions;
    private final long bulkSize;
    private final ElasticsearchHttpClient client;
    private final Jetty92RetryHelper retryHelper;
    private final ObjectMapper mapper;
    private final Logger log;
    private long totalCount;
    private int requestCount;
    private long requestBytes;
    private ArrayNode records;

    public ElasticsearchRecordBuffer(String attributeName, PluginTask task, Jetty92RetryHelper retryHelper)
    {
        this.attributeName = attributeName;
        this.task = task;
        this.bulkActions = task.getBulkActions();
        this.bulkSize = task.getBulkSize();
        this.client = new ElasticsearchHttpClient();
        this.retryHelper = retryHelper;
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
        this.records = JsonNodeFactory.instance.arrayNode();
        this.totalCount = 0;
        this.requestCount = 0;
        this.requestBytes = 0;
        this.log = Exec.getLogger(getClass());
    }

    @Override
    public void bufferRecord(ServiceRecord serviceRecord)
    {
        JacksonServiceRecord jacksonServiceRecord;
        try {
            jacksonServiceRecord = (JacksonServiceRecord) serviceRecord;
            JsonNode record = mapper.readTree(jacksonServiceRecord.toString()).get("record");

            requestCount++;
            totalCount++;
            requestBytes += record.toString().getBytes().length;

            records.add(record);
            if (requestCount >= bulkActions || requestBytes >= bulkSize) {
                client.push(records, task, retryHelper);
                if (totalCount % 10000 == 0) {
                    log.info("Inserted {} records", totalCount);
                }
                records = JsonNodeFactory.instance.arrayNode();
                requestBytes = 0;
                requestCount = 0;
            }
        }
        catch (ClassCastException ex) {
            throw new RuntimeException(ex);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public TaskReport commitWithTaskReportUpdated(TaskReport taskReport)
    {
        try {
            if (records.size() > 0) {
                client.push(records, task, retryHelper);
                log.info("Inserted {} records", records.size());
            }
            return Exec.newTaskReport().set("inserted", totalCount);
        }
        finally {
            this.retryHelper.close();
        }
    }
}
