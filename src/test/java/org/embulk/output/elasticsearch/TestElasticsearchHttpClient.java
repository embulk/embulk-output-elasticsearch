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
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.io.IOException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.time.Instant;

import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_ALIAS;
import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_INDEX;
import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_INDEX2;
import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_NODES;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;

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

        openSearchClient = utils.client();
    }

    @After
    public void close()
    {
        try {
            openSearchClient._transport().close();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY;
    private static final ConfigMapper CONFIG_MAPPER = ElasticsearchOutputPlugin.CONFIG_MAPPER;

    private ElasticsearchTestUtils utils;
    private OpenSearchClient openSearchClient;

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
        Instant time = Exec.getTransactionTime().getInstant();
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
        CreateIndexRequest request1 = new CreateIndexRequest.Builder().index(ES_INDEX).build();
        openSearchClient.indices().create(request1);
        CreateIndexRequest request2 = new CreateIndexRequest.Builder().index(ES_INDEX2).build();
        openSearchClient.indices().create(request2);

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
    public void testGetEsVersion()
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        final PluginTask task = CONFIG_MAPPER.map(utils.config(), PluginTask.class);
        assertThat(client.getEsVersion(task), is("2.6.0"));
    }
}
