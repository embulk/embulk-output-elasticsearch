package org.embulk.output.elasticsearch;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

public class ElasticsearchOutputPlugin
        implements OutputPlugin
{
    public interface NodeAddressTask
            extends Task
    {
        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("9300")
        public int getPort();
    }

    public interface PluginTask
            extends Task
    {
        @Config("version")
        @ConfigDefault("2")
        public Version getVersion();

        @Config("mode")
        @ConfigDefault("\"insert\"")
        public Mode getMode();

        @Config("nodes")
        public List<NodeAddressTask> getNodes();

        @Config("cluster_name")
        @ConfigDefault("\"elasticsearch\"")
        public String getClusterName();

        @Config("index")
        public String getIndex();
        public void setIndex(String indexName);

        @Config("alias")
        @ConfigDefault("null")
        public Optional<String> getAlias();
        public void setAlias(Optional<String> aliasName);

        @Config("index_type")
        public String getType();

        @Config("id")
        @ConfigDefault("null")
        public Optional<String> getId();

        @Config("bulk_actions")
        @ConfigDefault("1000")
        public int getBulkActions();

        @Config("bulk_size")
        @ConfigDefault("5242880")
        public long getBulkSize();

        @Config("concurrent_requests")
        @ConfigDefault("5")
        public int getConcurrentRequests();
    }

    public enum Version
    {
        ONE(1) {
            @Override
            public String getClientBuilderClassName()
            {
                return "org.embulk.output.elasticsearch.v1.ElasticsearchClient$Builder";
            }

            @Override
            public String getBulkProcessorBuilderClassName()
            {
                return "org.embulk.output.elasticsearch.v1.ElasticsearchBulkProcessor$Builder";
            }
        },
        TWO(2) {
            @Override
            public String getClientBuilderClassName()
            {
                return "org.embulk.output.elasticsearch.v2.ElasticsearchClient$Builder";
            }

            @Override
            public String getBulkProcessorBuilderClassName()
            {
                return "org.embulk.output.elasticsearch.v2.ElasticsearchBulkProcessor$Builder";
            }
        };

        private int version;

        Version(int version)
        {
            this.version = version;
        }

        @JsonValue
        public Integer toIntger()
        {
            return version;
        }

        @JsonCreator
        public static Version fromInteger(int value)
        {
            switch (value) {
                case 1:
                    return ONE;
                case 2:
                    return TWO;
                default:
                    throw new ConfigException(String.format("Version '%d' is not supported. Supported versions are 1 and 2.", value));
            }
        }

        public URL[] getUrls()
        {
            return listJarfiles(this);
        }

        public abstract String getClientBuilderClassName();
        public abstract String getBulkProcessorBuilderClassName();
    }

    private static URL[] listJarfiles(Version v)
    {
        final Path directory = getDirectory(v);
        try {
            final ImmutableList.Builder<URL> builder = ImmutableList.builder();
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>()
            {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs)
                {
                    if (path.toString().endsWith(".jar")) {
                        try {
                            builder.add(path.toUri().toURL());
                        }
                        catch (MalformedURLException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
            return builder.build().toArray(new URL[0]);
        }
        catch (IOException e) {
            throw new RuntimeException(String.format("Failed get a list of jar files at '%s'", directory), e);
        }
    }

    private static Path getDirectory(Version v)
    {
        String moduleVariable = "Embulk::Output::Elasticsearch::V_" + Integer.toString(v.version);
        return Paths.get((String) Exec.getInjector().getInstance(ScriptingContainer.class).runScriptlet(moduleVariable)).normalize();
    }

    public enum Mode
    {
        INSERT, REPLACE;

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

    private final Logger log;

    @Inject
    public ElasticsearchOutputPlugin()
    {
        log = Exec.getLogger(getClass());
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, Schema schema, int processorCount, Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);

        if (task.getMode().equals(Mode.REPLACE) && task.getVersion().equals(Version.ONE)) {
            throw new ConfigException("Not support replace mode on Elasticserach v1"); // TODO should support it
        }

        final URLClassLoader cl = newURLClassLoader(task);

        // confirm that a client can be initialized
        try (ElasticsearchClient client = newClient(task, cl)) {
            log.info(String.format("Executing plugin with '%s' mode.", task.getMode()));
            if (task.getMode().equals(Mode.REPLACE)) {
                task.setAlias(Optional.of(task.getIndex()));
                task.setIndex(generateNewIndexName(task.getIndex()));
                if (client.isExistsIndex(task.getAlias().orNull()) && !client.isAlias(task.getAlias().orNull())) {
                    throw new ConfigException(String.format("Invalid alias name [%s], an index exists with the same name as the alias", task.getAlias().orNull()));
                }
            }
            log.info(String.format("Inserting data into index[%s]", task.getIndex()));
            control.run(task.dump());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        return newConfigDiff();
    }

    private ConfigDiff newConfigDiff()
    {
        return Exec.newConfigDiff(); // TODO
    }

    @Override
    public  ConfigDiff resume(TaskSource taskSource,
                             Schema schema, int processorCount,
                             OutputPlugin.Control control)
    {
        return newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        Schema schema, int processorCount,
                        List<TaskReport> successTaskReports)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        if (task.getMode().equals(Mode.REPLACE)) {
            final URLClassLoader cl = newURLClassLoader(task);
            try (ElasticsearchClient client = newClient(task, cl)) {
                client.reassignAlias(task.getAlias().orNull(), task.getIndex());
            }
        }
    }

    private ElasticsearchClient newClient(PluginTask task, URLClassLoader cl)
    {
        String clientName = task.getVersion().getClientBuilderClassName();
        ElasticsearchClient.Builder builder = (ElasticsearchClient.Builder) newInstance(clientName, cl);
        return builder.setLogger(log).build(task); // TODO
    }

    private ElasticsearchBulkProcessor newBulkProcessor(PluginTask task, ElasticsearchClient client, URLClassLoader cl)
    {
        String bulkProcessorName = task.getVersion().getBulkProcessorBuilderClassName();
        ElasticsearchBulkProcessor.Builder builder = (ElasticsearchBulkProcessor.Builder) newInstance(bulkProcessorName, cl);
        return builder.setLogger(log).setClient(client).build(task);
    }

    private URLClassLoader newURLClassLoader(PluginTask task)
    {
        return URLClassLoader.newInstance(task.getVersion().getUrls(), this.getClass().getClassLoader());
    }

    private static Object newInstance(String name, URLClassLoader cl)
    {
        try {
            return Class.forName(name, true, cl).newInstance();
        }
        catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new ConfigException(e);
        }
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema,
                                        int processorIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final URLClassLoader cl = newURLClassLoader(task);

        ElasticsearchClient client = newClient(task, cl);
        ElasticsearchBulkProcessor bulkProcessor = newBulkProcessor(task, client, cl);
        ElasticsearchPageOutput pageOutput = new ElasticsearchPageOutput(task, client, bulkProcessor);
        pageOutput.open(schema);
        return pageOutput;
    }

    public static class ElasticsearchPageOutput implements TransactionalPageOutput
    {
        private Logger log;

        private ElasticsearchClient client;
        private ElasticsearchBulkProcessor bulkProcessor;

        private PageReader pageReader;
        private Column idColumn;

        private final String id;

        public ElasticsearchPageOutput(PluginTask task, ElasticsearchClient client, ElasticsearchBulkProcessor bulkProcessor)
        {
            this.log = Exec.getLogger(getClass());

            this.client = client;
            this.bulkProcessor = bulkProcessor;

            this.id = task.getId().orNull();
        }

        void open(final Schema schema)
        {
            pageReader = new PageReader(schema);
            idColumn = (id == null) ? null : schema.lookupColumn(id);
        }

        @Override
        public void add(Page page)
        {
            pageReader.setPage(page);

            while (pageReader.nextRecord()) {
                try {
                    bulkProcessor.addIndexRequest(pageReader, idColumn);
                } catch (IOException e) {
                    Throwables.propagate(e); //  TODO error handling
                }
            }
        }

        @Override
        public void finish()
        {
            try {
                bulkProcessor.flush();
            } finally {
                close();
            }
        }

        @Override
        public void close()
        {
            if (bulkProcessor != null) {
                try {
                    while (!bulkProcessor.awaitClose(3, TimeUnit.SECONDS)) {
                        log.debug("wait for closing the bulk processing..");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                bulkProcessor = null;
            }

            if (client != null) {
                client.close(); //  ElasticsearchException
                client = null;
            }
        }

        @Override
        public void abort()
        {
            // do nothing
        }

        @Override
        public TaskReport commit()
        {
            return Exec.newTaskReport(); // TODO
        }

    }

    public String generateNewIndexName(String indexName)
    {
        Timestamp time = Exec.getTransactionTime();
        return indexName + new SimpleDateFormat("_yyyyMMdd-HHmmss").format(time.toEpochMilli());
    }
}
