package org.embulk.output.elasticsearch;

import org.embulk.base.restclient.RestClientOutputPluginBase;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;

public class ElasticsearchOutputPlugin
        extends RestClientOutputPluginBase<ElasticsearchOutputPluginDelegate.PluginTask>
{
    public ElasticsearchOutputPlugin()
    {
        super(CONFIG_MAPPER_FACTORY, ElasticsearchOutputPluginDelegate.PluginTask.class, new ElasticsearchOutputPluginDelegate());
    }

    static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
    static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
}
