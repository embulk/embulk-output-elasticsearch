/*
 * Copyright 2015 The Embulk project
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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.AuthMethod;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.Mode;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.test.PageTestUtils;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_INDEX;
import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_INDEX_TYPE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestElasticsearchOutputPluginJSON
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY;
    private static final ConfigMapper CONFIG_MAPPER = ElasticsearchOutputPlugin.CONFIG_MAPPER;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private ElasticsearchOutputPlugin plugin;
    private ElasticsearchTestUtils utils;

    @Before
    public void createResources() throws Exception
    {
        utils = new ElasticsearchTestUtils();
        utils.initializeConstant();
        final PluginTask task = CONFIG_MAPPER.map(utils.configJSON(), PluginTask.class);
        utils.prepareBeforeTest(task);

        plugin = new ElasticsearchOutputPlugin();
    }

    @Test
    public void testDefaultValues()
    {
        final PluginTask task = CONFIG_MAPPER.map(utils.configJSON(), PluginTask.class);
        assertThat(task.getIndex(), is(ES_INDEX));
    }

    @Test
    public void testTransaction()
    {
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
        plugin.transaction(config, schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(CONFIG_MAPPER_FACTORY.newTaskReport());
            }
        });
        // no error happens
    }

    @Test
    public void testResume()
    {
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        plugin.resume(task.dump(), schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(CONFIG_MAPPER_FACTORY.newTaskReport());
            }
        });
    }

    @Test
    public void testCleanup()
    {
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        plugin.cleanup(task.dump(), schema, 0, Arrays.asList(CONFIG_MAPPER_FACTORY.newTaskReport()));
        // no error happens
    }

    @Test
    public void testOutputByOpen() throws Exception
    {
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        plugin.transaction(config, schema, 0, new OutputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(CONFIG_MAPPER_FACTORY.newTaskReport());
            }
        });
        TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);

        List<Page> pages = PageTestUtils.buildPage(Exec.getBufferAllocator(), schema, 1L, 32864L, "2015-01-27 19:23:49", "2015-01-27",  true, 123.45, "embulk");
        assertThat(pages.size(), is(1));
        for (Page page : pages) {
            output.add(page);
        }

        output.finish();
        output.commit();
        Thread.sleep(1500); // Need to wait until index done

        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        Method sendRequest = ElasticsearchHttpClient.class.getDeclaredMethod("sendRequest", String.class, HttpMethod.class, PluginTask.class, String.class);
        sendRequest.setAccessible(true);
        int esMajorVersion = client.getEsMajorVersion(task);
        String path = esMajorVersion >= ElasticsearchHttpClient.ES_SUPPORT_TYPELESS_API_VERSION
            ? String.format("/%s/_search", ES_INDEX)
            : String.format("/%s/%s/_search", ES_INDEX, ES_INDEX_TYPE);
        String sort = "{\"sort\" : \"id\"}";
        JsonNode response = (JsonNode) sendRequest.invoke(client, path, HttpMethod.POST, task, sort);

        int totalHits = esMajorVersion >= ElasticsearchTestUtils.ES_MIN_API_VERSION
            ? response.get("hits").get("total").get("value").asInt()
            : response.get("hits").get("total").asInt();

        assertThat(totalHits, is(1));

        if (response.size() > 0) {
            JsonNode record = response.get("hits").get("hits").get(0).get("_source");
            assertThat(record.get("id").asInt(), is(1));
            assertThat(record.get("account").asInt(), is(32864));
            assertThat(record.get("time").asText(), is("2015-01-27 19:23:49"));
            assertThat(record.get("purchase").asText(), is("2015-01-27"));
            assertThat(record.get("flg").asBoolean(), is(true));
            assertThat(record.get("score").asDouble(), is(123.45));
            assertThat(record.get("comment").asText(), is("embulk"));
        }
    }

    @Test
    public void testOutputByOpenWithNulls() throws Exception
    {
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        plugin.transaction(config, schema, 0, new OutputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(CONFIG_MAPPER_FACTORY.newTaskReport());
            }
        });
        TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);

        List<Page> pages = PageTestUtils.buildPage(Exec.getBufferAllocator(), schema, 2L, null, null, "2015-01-27",  true, 123.45, "embulk");
        assertThat(pages.size(), is(1));
        for (Page page : pages) {
            output.add(page);
        }

        output.finish();
        output.commit();
        Thread.sleep(1500); // Need to wait until index done

        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        Method sendRequest = ElasticsearchHttpClient.class.getDeclaredMethod("sendRequest", String.class, HttpMethod.class, PluginTask.class, String.class);
        sendRequest.setAccessible(true);
        int esMajorVersion = client.getEsMajorVersion(task);

        String path = esMajorVersion >= ElasticsearchHttpClient.ES_SUPPORT_TYPELESS_API_VERSION
            ? String.format("/%s/_search", ES_INDEX)
            : String.format("/%s/%s/_search", ES_INDEX, ES_INDEX_TYPE);
        String sort = "{\"sort\" : \"id\"}";

        JsonNode response = (JsonNode) sendRequest.invoke(client, path, HttpMethod.POST, task, sort);

        int totalHits = esMajorVersion >= ElasticsearchTestUtils.ES_MIN_API_VERSION
            ? response.get("hits").get("total").get("value").asInt()
            : response.get("hits").get("total").asInt();

        assertThat(totalHits, is(1));
        if (response.size() > 0) {
            JsonNode record = response.get("hits").get("hits").get(0).get("_source");
            assertThat(record.get("id").asInt(), is(2));
            assertTrue(record.get("account").isNull());
            assertTrue(record.get("time").isNull());
            assertThat(record.get("purchase").asText(), is("2015-01-27"));
            assertThat(record.get("flg").asBoolean(), is(true));
            assertThat(record.get("score").asDouble(), is(123.45));
            assertThat(record.get("comment").asText(), is("embulk"));
        }
    }

    @Test
    public void testOpenAbort()
    {
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);
        output.abort();
        // no error happens.
    }

    @Test
    public void testMode()
    {
        assertThat(Mode.values().length, is(2));
        assertThat(Mode.valueOf("INSERT"), is(Mode.INSERT));
    }

    @Test
    public void testAuthMethod()
    {
        assertThat(AuthMethod.values().length, is(2));
        assertThat(AuthMethod.valueOf("BASIC"), is(AuthMethod.BASIC));
    }

    @Test(expected = ConfigException.class)
    public void testModeThrowsConfigException()
    {
        Mode.fromString("non-exists-mode");
    }
}
