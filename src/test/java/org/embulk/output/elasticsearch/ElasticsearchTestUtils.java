package org.embulk.output.elasticsearch;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.embulk.config.ConfigSource;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.util.retryhelper.jetty92.Jetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;

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

    public static String ES_TEST_INDEX = "index_for_unittest";
    public static String ES_TEST_INDEX2 = "index_for_unittest2";
    public static String ES_TEST_ALIAS = "alias_for_unittest";

    /*
     * This test case requires environment variables
     *   ES_HOST
     *   ES_INDEX
     *   ES_INDEX_TYPE
     */
    public void initializeConstant()
    {
        ES_HOST = System.getenv("ES_HOST") != null ? System.getenv("ES_HOST") : "";
        ES_PORT = System.getenv("ES_PORT") != null ? Integer.valueOf(System.getenv("ES_PORT")) : 9200;

        ES_INDEX = System.getenv("ES_INDEX");
        ES_INDEX_TYPE = System.getenv("ES_INDEX_TYPE");
        ES_ID = "id";
        ES_BULK_ACTIONS = System.getenv("ES_BULK_ACTIONS") != null ? Integer.valueOf(System.getenv("ES_BULK_ACTIONS")) : 1000;
        ES_BULK_SIZE = System.getenv("ES_BULK_SIZE") != null ? Integer.valueOf(System.getenv("ES_BULK_SIZE")) : 5242880;
        ES_CONCURRENT_REQUESTS = System.getenv("ES_CONCURRENT_REQUESTS") != null ? Integer.valueOf(System.getenv("ES_CONCURRENT_REQUESTS")) : 5;

        assumeNotNull(ES_HOST, ES_INDEX, ES_INDEX_TYPE);

        ES_NODES = Arrays.asList(ImmutableMap.of("host", ES_HOST, "port", ES_PORT));

        PATH_PREFIX = ElasticsearchTestUtils.class.getClassLoader().getResource("sample_01.csv").getPath();
    }

    public void prepareBeforeTest(PluginTask task) throws Exception
    {
        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        try (Jetty92RetryHelper retryHelper = createRetryHelper()) {
            Method deleteIndex = ElasticsearchHttpClient.class.getDeclaredMethod("deleteIndex", String.class, PluginTask.class, Jetty92RetryHelper.class);
            deleteIndex.setAccessible(true);

            // Delete alias
            if (client.isAliasExisting(ES_TEST_ALIAS, task, retryHelper)) {
                deleteIndex.invoke(client, ES_TEST_ALIAS, task, retryHelper);
            }

            // Delete index
            if (client.isIndexExisting(ES_TEST_INDEX, task, retryHelper)) {
                deleteIndex.invoke(client, ES_TEST_INDEX, task, retryHelper);
            }

            if (client.isIndexExisting(ES_TEST_INDEX2, task, retryHelper)) {
                deleteIndex.invoke(client, ES_TEST_INDEX2, task, retryHelper);
            }
        }
    }

    public ConfigSource config()
    {
        return Exec.newConfigSource()
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
                .set("concurrent_requests", ES_CONCURRENT_REQUESTS);
    }

    public ImmutableMap<String, Object> inputConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", PATH_PREFIX);
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

    public Jetty92RetryHelper createRetryHelper()
    {
        return new Jetty92RetryHelper(
                2,
                1000,
                32000,
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
