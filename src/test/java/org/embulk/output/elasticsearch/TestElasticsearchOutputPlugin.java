package org.embulk.output.elasticsearch;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.security.GeneralSecurityException;

import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.standards.CsvParserPlugin;
import org.embulk.output.elasticsearch.ElasticsearchOutputPlugin.PluginTask;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

public class TestElasticsearchOutputPlugin
{
    private static String ES_HOST;
    private static int ES_PORT;
    private static List ES_NODES;
    private static String ES_CLUSTER_NAME;
    private static String ES_INDEX;
    private static String ES_INDEX_TYPE;
    private static String ES_ID;
    private static int ES_BULK_ACTIONS;
    private static int ES_BULK_SIZE;
    private static int ES_CONCURRENT_REQUESTS;
    private static String PATH_PREFIX;

    private MockPageOutput pageOutput;

    final String ES_TEST_INDEX = "index_for_unittest";
    final String ES_TEST_INDEX2 = "index_for_unittest2";
    final String ES_TEST_ALIAS = "alias_for_unittest";

    /*
     * This test case requires environment variables
     *   ES_HOST
     *   ES_INDEX
     *   ES_INDEX_TYPE
     */
    @BeforeClass
    public static void initializeConstant()
    {
        ES_HOST = System.getenv("ES_HOST") != null ? System.getenv("ES_HOST") : "";
        ES_PORT = System.getenv("ES_PORT") != null ? Integer.valueOf(System.getenv("ES_PORT")) : 9300;

        ES_CLUSTER_NAME = System.getenv("ES_CLUSTER_NAME") != null ? System.getenv("ES_CLUSTER_NAME") : "";
        ES_INDEX = System.getenv("ES_INDEX");
        ES_INDEX_TYPE = System.getenv("ES_INDEX_TYPE");
        ES_ID = "id";
        ES_BULK_ACTIONS = System.getenv("ES_BULK_ACTIONS") != null ? Integer.valueOf(System.getenv("ES_BULK_ACTIONS")) : 1000;
        ES_BULK_SIZE = System.getenv("ES_BULK_SIZE") != null ? Integer.valueOf(System.getenv("ES_BULK_SIZE")) : 5242880;
        ES_CONCURRENT_REQUESTS = System.getenv("ES_CONCURRENT_REQUESTS") != null ? Integer.valueOf(System.getenv("ES_CONCURRENT_REQUESTS")) : 5;

        assumeNotNull(ES_HOST, ES_INDEX, ES_INDEX_TYPE);

        ES_NODES = Arrays.asList(ImmutableMap.of("host", ES_HOST, "port", ES_PORT));

        PATH_PREFIX = ElasticsearchOutputPlugin.class.getClassLoader().getResource("sample_01.csv").getPath();
    }


    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private ElasticsearchOutputPlugin plugin;

    @Before
    public void createResources()
            throws GeneralSecurityException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException
    {
        ConfigSource config = config();
        plugin = new ElasticsearchOutputPlugin();
        PluginTask task = config.loadConfig(PluginTask.class);
        pageOutput = new MockPageOutput();

        Method createClient = ElasticsearchOutputPlugin.class.getDeclaredMethod("createClient", PluginTask.class);
        createClient.setAccessible(true);
        try (Client client = (Client) createClient.invoke(plugin, task)) {
            // Delete alias
            if (client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().getMetaData().hasAlias(ES_TEST_ALIAS)) {
                client.admin().indices().delete(new DeleteIndexRequest(ES_TEST_ALIAS)).actionGet();
            }

            // Delete index
            if (client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().getMetaData().hasIndex(ES_TEST_INDEX)) {
                client.admin().indices().delete(new DeleteIndexRequest(ES_TEST_INDEX)).actionGet();
            }

            if (client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().getMetaData().hasIndex(ES_TEST_INDEX2)) {
                client.admin().indices().delete(new DeleteIndexRequest(ES_TEST_INDEX2)).actionGet();
            }
        }
    }

    @Test
    public void testDefaultValues()
    {
        ConfigSource config = config();
        ElasticsearchOutputPlugin.PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals(ES_INDEX, task.getIndex());
    }

    @Test
    public void testDefaultValuesNull()
    {
        ConfigSource config = Exec.newConfigSource()
            .set("in", inputConfig())
            .set("parser", parserConfig(schemaConfig()))
            .set("type", "elasticsearch")
            .set("mode", "") // NULL
            .set("nodes", ES_NODES)
            .set("cluster_name", ES_CLUSTER_NAME)
            .set("index", ES_INDEX)
            .set("index_type", ES_INDEX_TYPE)
            .set("id", ES_ID)
            .set("bulk_actions", ES_BULK_ACTIONS)
            .set("bulk_size", ES_BULK_SIZE)
            .set("concurrent_requests", ES_CONCURRENT_REQUESTS
            );
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        try {
            plugin.transaction(config, schema, 0, new OutputPlugin.Control()
            {
                @Override
                public List<TaskReport> run(TaskSource taskSource)
                {
                    return Lists.newArrayList(Exec.newTaskReport());
                }
            });
        } catch (Throwable t) {
            if (t instanceof RuntimeException) {
                assertTrue(t.getCause().getCause() instanceof ConfigException);
            }
        }
    }

    @Test
    public void testResume()
    {
        ConfigSource config = config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.resume(task.dump(), schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
    }

    @Test
    public void testTransaction()
    {
        ConfigSource config = Exec.newConfigSource()
            .set("in", inputConfig())
            .set("parser", parserConfig(schemaConfig()))
            .set("type", "elasticsearch")
            .set("mode", "replace")
            .set("nodes", ES_NODES)
            .set("cluster_name", ES_CLUSTER_NAME)
            .set("index", ES_INDEX)
            .set("index_type", ES_INDEX_TYPE)
            .set("id", ES_ID)
            .set("bulk_actions", ES_BULK_ACTIONS)
            .set("bulk_size", ES_BULK_SIZE)
            .set("concurrent_requests", ES_CONCURRENT_REQUESTS
            );
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        plugin.transaction(config, schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
        // no error happens
    }

    @Test
    public void testOutputByOpen()
            throws GeneralSecurityException, IOException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, ParseException
    {
        ConfigSource config = config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.transaction(config, schema, 0, new OutputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource) {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
        TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);

        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, 1L, 32864L, Timestamp.ofEpochSecond(1422386629), Timestamp.ofEpochSecond(1422316800),  true, 123.45, "embulk");
        assertEquals(1, pages.size());
        for (Page page : pages) {
            output.add(page);
        }

        output.finish();
        output.commit();

        Method createClient = ElasticsearchOutputPlugin.class.getDeclaredMethod("createClient", PluginTask.class);
        createClient.setAccessible(true);
        try (Client client = (Client) createClient.invoke(plugin, task)) {
            GetResponse response = client.prepareGet(ES_INDEX, ES_INDEX_TYPE, "1").execute().actionGet();
            assertTrue(response.isExists());
            if (response.isExists()) {
                Map<String, Object> map = response.getSourceAsMap();
                assertEquals(1, map.get("id"));
                assertEquals(32864, map.get("account"));
                assertEquals("2015-01-27T19:23:49.000Z", map.get("time"));
                assertEquals("2015-01-27T00:00:00.000Z", map.get("purchase"));
                assertEquals(true, map.get("flg"));
                assertEquals(123.45, map.get("score"));
                assertEquals("embulk", map.get("comment"));
            }
        }
    }

    @Test
    public void testOpenAbort()
    {
        ConfigSource config = config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);
        output.abort();
        // no error happens.
    }

    @Test
    public void testCreateClientThrowsException()
            throws GeneralSecurityException, IOException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException
    {
        ConfigSource config = Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "elasticsearch")
                .set("mode", "replace")
                .set("nodes", Arrays.asList(ImmutableMap.of("host", "unknown-host", "port", 9300)))
                .set("cluster_name", ES_CLUSTER_NAME)
                .set("index", ES_INDEX)
                .set("index_type", ES_INDEX_TYPE)
                .set("id", ES_ID)
                .set("bulk_actions", ES_BULK_ACTIONS)
                .set("bulk_size", ES_BULK_SIZE)
                .set("concurrent_requests", ES_CONCURRENT_REQUESTS
                );
        PluginTask task = config.loadConfig(PluginTask.class);

        Method createClient = ElasticsearchOutputPlugin.class.getDeclaredMethod("createClient", PluginTask.class);
        createClient.setAccessible(true);
        try (Client client = (Client) createClient.invoke(plugin, task)) {
        } catch (Throwable t) {
            if (t instanceof InvocationTargetException) {
                assertTrue(t.getCause().getCause() instanceof UnknownHostException);
            }
        }
    }

    @Test
    public void testMode()
    {
        assertEquals(2, ElasticsearchOutputPlugin.Mode.values().length);
        assertEquals(ElasticsearchOutputPlugin.Mode.INSERT, ElasticsearchOutputPlugin.Mode.valueOf("INSERT"));
    }

    @Test(expected = ConfigException.class)
    public void testModeThrowsConfigException()
    {
        ElasticsearchOutputPlugin.Mode.fromString("non-exists-mode");
    }

    @Test
    public void testDeleteIndex()
            throws GeneralSecurityException, IOException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException
    {
        ConfigSource config = config();
        PluginTask task = config.loadConfig(PluginTask.class);

        Method createClient = ElasticsearchOutputPlugin.class.getDeclaredMethod("createClient", PluginTask.class);
        createClient.setAccessible(true);
        try (Client client = (Client) createClient.invoke(plugin, task)) {
            // Create Index
            client.admin().indices().create(new CreateIndexRequest(ES_TEST_INDEX)).actionGet();

            Method deleteIndex = ElasticsearchOutputPlugin.class.getDeclaredMethod("deleteIndex", String.class, Client.class);
            deleteIndex.setAccessible(true);
            deleteIndex.invoke(plugin, ES_TEST_INDEX, client);

            assertEquals(false, client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().getMetaData().hasIndex(ES_TEST_INDEX));
        }
    }

    @Test
    public void testAlias()
            throws GeneralSecurityException, IOException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException
    {
        ConfigSource config = config();
        PluginTask task = config.loadConfig(PluginTask.class);

        Method createClient = ElasticsearchOutputPlugin.class.getDeclaredMethod("createClient", PluginTask.class);
        createClient.setAccessible(true);
        try (Client client = (Client) createClient.invoke(plugin, task)) {

            Method isAlias = ElasticsearchOutputPlugin.class.getDeclaredMethod("isAlias", String.class, Client.class);
            isAlias.setAccessible(true);

            Method isExistsAlias = ElasticsearchOutputPlugin.class.getDeclaredMethod("isExistsAlias", String.class, Client.class);
            isExistsAlias.setAccessible(true);

            Method getIndexByAlias = ElasticsearchOutputPlugin.class.getDeclaredMethod("getIndexByAlias", String.class, Client.class);
            getIndexByAlias.setAccessible(true);

            Method reAssignAlias = ElasticsearchOutputPlugin.class.getDeclaredMethod("reAssignAlias", String.class, String.class, Client.class);
            reAssignAlias.setAccessible(true);

            assertEquals(false, isAlias.invoke(plugin, ES_TEST_ALIAS, client));
            assertEquals(false, isExistsAlias.invoke(plugin, ES_TEST_ALIAS, client));
            List<String> indicesBefore = (List<String>) getIndexByAlias.invoke(plugin, ES_TEST_ALIAS, client);
            assertEquals(0, indicesBefore.size());

            // Create Index
            client.admin().indices().create(new CreateIndexRequest(ES_TEST_INDEX)).actionGet();
            client.admin().indices().create(new CreateIndexRequest(ES_TEST_INDEX2)).actionGet();
            // Assign Alias
            reAssignAlias.invoke(plugin, ES_TEST_ALIAS, ES_TEST_INDEX, client);

            assertEquals(true, isAlias.invoke(plugin, ES_TEST_ALIAS, client));
            assertEquals(true, isExistsAlias.invoke(plugin, ES_TEST_ALIAS, client));
            List<String> indicesAfter = (List<String>) getIndexByAlias.invoke(plugin, ES_TEST_ALIAS, client);
            assertEquals(1, indicesAfter.size());

            // ReAssginAlias
            reAssignAlias.invoke(plugin, ES_TEST_ALIAS, ES_TEST_INDEX2, client);
            List<String> indicesReassign = (List<String>) getIndexByAlias.invoke(plugin, ES_TEST_ALIAS, client);
            assertEquals(1, indicesReassign.size());
        }
    }

    @Test
    public void testIsExistsIndex()
            throws GeneralSecurityException, IOException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException
    {
        ConfigSource config = config();
        PluginTask task = config.loadConfig(PluginTask.class);

        Method createClient = ElasticsearchOutputPlugin.class.getDeclaredMethod("createClient", PluginTask.class);
        createClient.setAccessible(true);
        try (Client client = (Client) createClient.invoke(plugin, task)) {
            Method isExistsIndex = ElasticsearchOutputPlugin.class.getDeclaredMethod("isExistsIndex", String.class, Client.class);
            isExistsIndex.setAccessible(true);

            // Delete index
            if (client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().getMetaData().hasIndex(ES_TEST_INDEX)) {
                client.admin().indices().delete(new DeleteIndexRequest(ES_TEST_INDEX)).actionGet();
            }
            assertEquals(false, isExistsIndex.invoke(plugin, ES_TEST_INDEX, client));

            // Create Index
            client.admin().indices().create(new CreateIndexRequest(ES_TEST_INDEX)).actionGet();
            assertEquals(true, isExistsIndex.invoke(plugin, ES_TEST_INDEX, client));
        }
    }

    @Test
    public void testGenerateNewIndex()
    {
        String newIndexName = plugin.generateNewIndexName(ES_INDEX);
        Timestamp time = Exec.getTransactionTime();
        assertEquals(ES_INDEX + new SimpleDateFormat("_yyyyMMdd-HHmmss").format(time.toEpochMilli()), newIndexName);
    }

    private byte[] convertInputStreamToByte(InputStream is) throws IOException
    {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        byte [] buffer = new byte[1024];
        while(true) {
            int len = is.read(buffer);
            if(len < 0) {
                break;
            }
            bo.write(buffer, 0, len);
        }
        return bo.toByteArray();
    }

    private ConfigSource config()
    {
        return Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "elasticsearch")
                .set("mode", "insert")
                .set("nodes", ES_NODES)
                .set("cluster_name", ES_CLUSTER_NAME)
                .set("index", ES_INDEX)
                .set("index_type", ES_INDEX_TYPE)
                .set("id", ES_ID)
                .set("bulk_actions", ES_BULK_ACTIONS)
                .set("bulk_size", ES_BULK_SIZE)
                .set("concurrent_requests", ES_CONCURRENT_REQUESTS);
    }

    private ImmutableMap<String, Object> inputConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", PATH_PREFIX);
        builder.put("last_path", "");
        return builder.build();
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
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

    private ImmutableList<Object> schemaConfig()
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
}
