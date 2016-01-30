package org.embulk.output.elasticsearch;

import org.slf4j.Logger;

public interface ElasticsearchClient
    extends AutoCloseable
{
    interface Builder
    {
        Builder setLogger(Logger log);
        ElasticsearchClient build(ElasticsearchOutputPlugin.PluginTask task);
    }

    void reassignAlias(String aliasName, String newIndexName);
    boolean isExistsIndex(String indexName);
    boolean isAlias(String aliasName);
    void close();
}
