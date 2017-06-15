package org.embulk.output.elasticsearch;

import org.eclipse.jetty.http.HttpMethod;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.time.Timestamp;
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

        PluginTask task = utils.config().loadConfig(PluginTask.class);
        utils.prepareBeforeTest(task);
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
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
        PluginTask task = utils.config().loadConfig(PluginTask.class);
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
        PluginTask task = utils.config().loadConfig(PluginTask.class);
        assertThat(client.isIndexExisting("non-existing-index", task), is(false));
    }

    @Test
    public void testIsAliasExistingWithNonExistsAlias()
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        PluginTask task = utils.config().loadConfig(PluginTask.class);
        assertThat(client.isAliasExisting("non-existing-alias", task), is(false));
    }

    @Test
    public void testGetAuthorizationHeader() throws Exception
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();

        ConfigSource config = Exec.newConfigSource()
                .set("auth_method", "basic")
                .set("user", "username")
                .set("password", "password")
                .set("index", "idx")
                .set("index_type", "idx_type")
                .set("nodes", ES_NODES);

        assertThat(client.getAuthorizationHeader(config.loadConfig(PluginTask.class)), is("Basic dXNlcm5hbWU6cGFzc3dvcmQ="));
    }
}
