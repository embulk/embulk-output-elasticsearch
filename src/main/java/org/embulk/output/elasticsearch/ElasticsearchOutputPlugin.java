package org.embulk.output.elasticsearch;

import org.embulk.base.restclient.RestClientOutputPluginBase;

public class ElasticsearchOutputPlugin
        extends RestClientOutputPluginBase<ElasticsearchOutputPluginDelegate.PluginTask>
{
    public ElasticsearchOutputPlugin()
    {
        super(ElasticsearchOutputPluginDelegate.PluginTask.class, new ElasticsearchOutputPluginDelegate());
    }
}
