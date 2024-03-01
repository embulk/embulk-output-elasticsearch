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

import org.eclipse.jetty.http.HttpMethod;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.time.Timestamp;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.Method;
import java.text.SimpleDateFormat;

import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_ALIAS;
import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_INDEX;
import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_INDEX2;
import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_NODES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class TestElasticsearchHttpClient
{
    @BeforeClass
    public static void initializeConstant()
    {
    }

    @Before
    public void createResources() throws Exception
    {
        utils = new ElasticsearchTestUtils();
        utils.initializeConstant();

        final PluginTask task = CONFIG_MAPPER.map(utils.config(), PluginTask.class);
        utils.prepareBeforeTest(task);
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY;
    private static final ConfigMapper CONFIG_MAPPER = ElasticsearchOutputPlugin.CONFIG_MAPPER;

    private ElasticsearchTestUtils utils;

    @Test
    public void testValidateIndexOrAliasName()
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        client.validateIndexOrAliasName("embulk", "index");
    }

    @Test(expected = ConfigException.class)
    public void testIndexNameContainsUpperCase()
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        client.validateIndexOrAliasName("Embulk", "index");
    }

    @Test(expected = ConfigException.class)
    public void testIndexNameStartsInvalidChars()
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        client.validateIndexOrAliasName("_embulk", "index");
    }

    @Test(expected = ConfigException.class)
    public void testIndexNameContainsInvalidChars()
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        client.validateIndexOrAliasName("em#bulk", "index");
    }

    @Test(expected = ConfigException.class)
    public void testIndexNameTooLong()
    {
        String index = "embulk";
        for (int i = 0; i < 255; i++) {
            index += "s";
        }
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        client.validateIndexOrAliasName(index, "index");
    }

    @Test(expected = ConfigException.class)
    public void testIndexNameEqDot()
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        client.validateIndexOrAliasName(".", "index");
    }

    @Test
    public void testGenerateNewIndex()
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        String newIndexName = client.generateNewIndexName(ES_INDEX);
        Timestamp time = Exec.getTransactionTime();
        assertThat(newIndexName, is(ES_INDEX + new SimpleDateFormat("_yyyyMMdd-HHmmss").format(time.toEpochMilli())));
    }

    @Test
    public void testCreateAlias() throws Exception
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        final PluginTask task = CONFIG_MAPPER.map(utils.config(), PluginTask.class);
        // delete index
        Method method = ElasticsearchHttpClient.class.getDeclaredMethod("deleteIndex", String.class, PluginTask.class);
        method.setAccessible(true);
        method.invoke(client, "newindex", task);

        // create index
        Method sendRequest = ElasticsearchHttpClient.class.getDeclaredMethod("sendRequest", String.class, HttpMethod.class, PluginTask.class);
        sendRequest.setAccessible(true);
        String path = String.format("/%s/", ES_INDEX);
        sendRequest.invoke(client, path, HttpMethod.PUT, task);

        path = String.format("/%s/", ES_INDEX2);
        sendRequest.invoke(client, path, HttpMethod.PUT, task);

        // create alias
        client.reassignAlias(ES_ALIAS, ES_INDEX, task);

        // check alias
        assertThat(client.isAliasExisting(ES_ALIAS, task), is(true));
        assertThat(client.getIndexByAlias(ES_ALIAS, task).toString(), is("[" + ES_INDEX + "]"));

        // reassign index
        client.reassignAlias(ES_ALIAS, ES_INDEX2, task);
        assertThat(client.getIndexByAlias(ES_ALIAS, task).toString(), is("[" + ES_INDEX2 + "]"));
    }

    @Test
    public void testIsIndexExistingWithNonExistsIndex()
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        final PluginTask task = CONFIG_MAPPER.map(utils.config(), PluginTask.class);
        assertThat(client.isIndexExisting("non-existing-index", task), is(false));
    }

    @Test
    public void testIsAliasExistingWithNonExistsAlias()
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        final PluginTask task = CONFIG_MAPPER.map(utils.config(), PluginTask.class);
        assertThat(client.isAliasExisting("non-existing-alias", task), is(false));
    }

    @Test
    public void testGetAuthorizationHeader() throws Exception
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();

        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("auth_method", "basic")
                .set("user", "username")
                .set("password", "password")
                .set("index", "idx")
                .set("index_type", "idx_type")
                .set("nodes", ES_NODES);

        assertThat(
                client.getAuthorizationHeader(CONFIG_MAPPER.map(config, PluginTask.class)),
                is("Basic dXNlcm5hbWU6cGFzc3dvcmQ="));
    }
}
