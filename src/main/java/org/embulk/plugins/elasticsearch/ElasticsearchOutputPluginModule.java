package org.embulk.plugins.elasticsearch;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Extension;
import org.embulk.spi.OutputPlugin;

import java.util.List;

import static org.embulk.plugin.InjectedPluginSource.registerPluginTo;

public class ElasticsearchOutputPluginModule
        implements Extension, Module
{

    @Override
    public void configure(Binder binder)
    {
        Preconditions.checkNotNull(binder, "binder is null.");
        registerPluginTo(binder, OutputPlugin.class, "elasticsearch", ElasticsearchOutputPlugin.class);
    }

    @Override
    public List<Module> getModules(ConfigSource systemConfig)
    {
        return ImmutableList.<Module>of(this);
    }
}
