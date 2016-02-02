package org.embulk.output.elasticsearch;

import org.embulk.output.elasticsearch.ElasticsearchOutputPlugin.PluginTask;
import org.slf4j.Logger;

public interface ElasticsearchClient
    extends AutoCloseable
{
    abstract class Builder
    {
        protected Logger log;
        protected PluginTask task;

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

        public abstract ElasticsearchClient build();
    }

    void reassignAlias(String aliasName, String newIndexName);
    boolean isExistsIndex(String indexName);
    boolean isAlias(String aliasName);
    void close();
}
