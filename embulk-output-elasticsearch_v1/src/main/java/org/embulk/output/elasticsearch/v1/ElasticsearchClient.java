package org.embulk.output.elasticsearch.v1;

import com.google.common.base.Throwables;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.embulk.output.elasticsearch.ElasticsearchOutputPlugin.NodeAddressTask;
import org.embulk.output.elasticsearch.ElasticsearchOutputPlugin.PluginTask;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class ElasticsearchClient
        implements org.embulk.output.elasticsearch.ElasticsearchClient
{
    public static class Builder
            implements org.embulk.output.elasticsearch.ElasticsearchClient.Builder
    {
        private Logger log;

        public Builder setLogger(Logger log)
        {
            this.log = log;
            return this;
        }

        public org.embulk.output.elasticsearch.ElasticsearchClient build(PluginTask task)
        {
            //  @see http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/client.html
            Settings settings = ImmutableSettings.settingsBuilder()
                    .classLoader(Settings.class.getClassLoader())
                    .put("cluster.name", task.getClusterName())
                    .build();
            TransportClient client = new TransportClient(settings);
            List<NodeAddressTask> nodes = task.getNodes();
            for (NodeAddressTask node : nodes) {
                try {
                    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(node.getHost()), node.getPort()));
                } catch (UnknownHostException e) {
                    Throwables.propagate(e);
                }
            }
            return new ElasticsearchClient(log, client);
        }
    }

    private final Logger log;
    private final Client client;

    ElasticsearchClient(Logger log, Client client)
    {
        this.log = log;
        this.client = client;
    }

    Client getClient()
    {
        return client;
    }

    public void reassignAlias(String aliasName, String newIndexName)
    {
        throw new UnsupportedOperationException(); // TODO
    }

    public boolean isExistsIndex(String indexName)
    {
        return client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().getMetaData().hasIndex(indexName);
    }

    public boolean isAlias(String aliasName)
    {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public void close()
    {
        client.close();
    }

}
