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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.PluginTask;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assume.assumeNotNull;

public class ElasticsearchTestUtils
{
    public static String ES_HOST;
    public static int ES_PORT;
    public static List ES_NODES;
    public static String ES_INDEX;
    public static String ES_INDEX_TYPE;
    public static String ES_ID;
    public static int ES_BULK_ACTIONS;
    public static int ES_BULK_SIZE;
    public static int ES_CONCURRENT_REQUESTS;
    public static String PATH_PREFIX;
    public static String JSON_PATH_PREFIX;
    public static String ES_INDEX2;
    public static String ES_ALIAS;

    public void initializeConstant()
    {
        ES_HOST = "localhost";
        ES_PORT = 19200;

        ES_INDEX = "embulk";
        ES_INDEX2 = ES_INDEX + "_02";
        ES_ALIAS = ES_INDEX + "_alias";
        ES_INDEX_TYPE = "embulk";
        ES_ID = "id";
        ES_BULK_ACTIONS = 1000;
        ES_BULK_SIZE = 5242880;
        ES_CONCURRENT_REQUESTS = 5;

        ES_NODES = Arrays.asList(ImmutableMap.of("host", ES_HOST, "port", ES_PORT));

        PATH_PREFIX = ElasticsearchTestUtils.class.getClassLoader().getResource("sample_01.csv").getPath();
        JSON_PATH_PREFIX = ElasticsearchTestUtils.class.getClassLoader().getResource("sample_01.json").getPath();
    }

    public void prepareBeforeTest(PluginTask task) throws Exception
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        Method deleteIndex = ElasticsearchHttpClient.class.getDeclaredMethod("deleteIndex", String.class, PluginTask.class);
        deleteIndex.setAccessible(true);

        // Delete alias
        if (client.isAliasExisting(ES_ALIAS, task)) {
            deleteIndex.invoke(client, ES_ALIAS, task);
        }

        // Delete index
        if (client.isIndexExisting(ES_INDEX, task)) {
            deleteIndex.invoke(client, ES_INDEX, task);
        }

        if (client.isIndexExisting(ES_INDEX2, task)) {
            deleteIndex.invoke(client, ES_INDEX2, task);
        }
    }

    public ConfigSource config()
    {
        return ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "elasticsearch")
                .set("mode", "insert")
                .set("nodes", ES_NODES)
                .set("index", ES_INDEX)
                .set("index_type", ES_INDEX_TYPE)
                .set("id", ES_ID)
                .set("bulk_actions", ES_BULK_ACTIONS)
                .set("bulk_size", ES_BULK_SIZE)
                .set("concurrent_requests", ES_CONCURRENT_REQUESTS)
                .set("maximum_retries", 2);
    }

    public ConfigSource oldParserConfig(final EmbulkTestRuntime runtime)
    {
        return runtime.getExec().newConfigSource()
                .set("parser", parserConfig(schemaConfig()))
                .getNested("parser");
    }

    public ConfigSource configJSON()
    {
        return ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("in", inputConfigJSON())
                .set("parser", parserConfigJSON())
                .set("type", "elasticsearch")
                .set("mode", "insert")
                .set("nodes", ES_NODES)
                .set("index", ES_INDEX)
                .set("index_type", ES_INDEX_TYPE)
                .set("id", ES_ID)
                .set("bulk_actions", ES_BULK_ACTIONS)
                .set("bulk_size", ES_BULK_SIZE)
                .set("concurrent_requests", ES_CONCURRENT_REQUESTS)
                .set("maximum_retries", 2)
                .set("fill_null_for_empty_column", true);
    }

    public ImmutableMap<String, Object> inputConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", PATH_PREFIX);
        builder.put("last_path", "");
        return builder.build();
    }

    public ImmutableMap<String, Object> inputConfigJSON()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", JSON_PATH_PREFIX);
        builder.put("last_path", "");
        return builder.build();
    }

    public ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    public ImmutableMap<String, Object> parserConfigJSON()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        return builder.build();
    }

    public ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "long"));
        builder.add(ImmutableMap.of("name", "time", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "flg", "type", "boolean"));
        builder.add(ImmutableMap.of("name", "score", "type", "double"));
        builder.add(ImmutableMap.of("name", "comment", "type", "string"));
        return builder.build();
    }

    public Schema JSONSchema()
    {
        return Schema.builder()
                .add("id", Types.LONG)
                .add("account", Types.LONG)
                .add("time", Types.STRING)
                .add("purchase", Types.STRING)
                .add("flg", Types.BOOLEAN)
                .add("score", Types.DOUBLE)
                .add("comment", Types.STRING)
                .build();
    }
}
