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
    public abstract class Builder
    {
        protected Logger log;
        protected PluginTask task;
        protected ElasticsearchClient client;

        public Builder setLogger(Logger log)
        {
            this.log = log;
            return this;
        }

        public Builder setPluginTask(PluginTask task)
        {
            this.task = task;
            return this;
        }

        public Builder setClient(ElasticsearchClient client)
        {
            this.client = client;
            return this;
        }

        public abstract ElasticsearchBulkProcessor build();
    }

    void addIndexRequest(PageReader pageReader, Column idColumn) throws IOException;
    void flush();
    boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException;

}
