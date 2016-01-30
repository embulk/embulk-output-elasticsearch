package org.embulk.output.elasticsearch;

import org.embulk.output.elasticsearch.ElasticsearchOutputPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public interface ElasticsearchBulkProcessor
        extends Closeable
{
    interface Builder
    {
        Builder setLogger(Logger log);
        Builder setClient(ElasticsearchClient client);
        ElasticsearchBulkProcessor build(PluginTask task);
    }

    void addIndexRequest(PageReader pageReader, Column idColumn) throws IOException;
    void flush();
    boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException;
}
