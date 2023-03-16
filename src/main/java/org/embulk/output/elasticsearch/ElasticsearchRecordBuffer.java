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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import jakarta.json.JsonObject;
import jakarta.json.stream.JsonParser;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.config.TaskReport;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.PluginTask;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.JsonpMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

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
    private final ObjectMapper mapper;
    private final JsonpMapper jsonpMapper;
    private final Logger log;
    private long totalCount;
    private int requestCount;
    private long requestBytes;
    private List<JsonData> records;

    public ElasticsearchRecordBuffer(String attributeName, PluginTask task)
    {
        this.attributeName = attributeName;
        this.task = task;
        this.bulkActions = task.getBulkActions();
        this.bulkSize = task.getBulkSize();
        this.client = new ElasticsearchHttpClient();
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
        this.jsonpMapper = new JacksonJsonpMapper(this.mapper);
        this.records = new ArrayList<>();
        this.totalCount = 0;
        this.requestCount = 0;
        this.requestBytes = 0;
        this.log = LoggerFactory.getLogger(getClass());
    }

    @Override
    public void bufferRecord(ServiceRecord serviceRecord)
    {
        JacksonServiceRecord jacksonServiceRecord;
        try {
            jacksonServiceRecord = (JacksonServiceRecord) serviceRecord;

            // TODO: performance
            JsonData jsonData = parseServiceRecord(serviceRecord.toString());
            JsonObject jsonObject = jsonData.toJson(jsonpMapper).asJsonObject().getJsonObject("record");
            String jsonString = jsonObject.toString();
            JsonParser parser = jsonpMapper.jsonProvider().createParser(new StringReader(jsonString));
            JsonData record = JsonData.from(parser, jsonpMapper);

            requestCount++;
            totalCount++;
            requestBytes += jsonString.getBytes().length;

            records.add(record);
            if (requestCount >= bulkActions || requestBytes >= bulkSize) {
                client.push(records, task);
                if (totalCount % 10000 == 0) {
                    log.info("Inserted {} records", totalCount);
                }
                records = new ArrayList<>();
                requestBytes = 0;
                requestCount = 0;
            }
        }
        catch (ClassCastException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void finish()
    {
    }

    @Override
    public void close()
    {
    }

    @Override
    public TaskReport commitWithTaskReportUpdated(TaskReport taskReport)
    {
        if (records.size() > 0) {
            client.push(records, task);
            log.info("Inserted {} records", records.size());
        }
        return ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY.newTaskReport().set("inserted", totalCount);
    }

    private JsonData parseServiceRecord(String serviceRecordString)
    {
        JsonParser parser = jsonpMapper.jsonProvider().createParser(new StringReader(serviceRecordString));
        return JsonData.from(parser, jsonpMapper);
    }
}
