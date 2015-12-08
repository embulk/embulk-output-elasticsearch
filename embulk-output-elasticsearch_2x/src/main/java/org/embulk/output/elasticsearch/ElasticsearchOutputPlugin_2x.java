package org.embulk.output.elasticsearch;

import java.util.List;
import java.net.InetAddress;
import java.net.UnknownHostException;
import com.google.common.base.Throwables;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ElasticsearchOutputPlugin_2x
        extends AbstractElasticsearchOutputPlugin
{
    @Override
    protected Client createClient(final PluginTask task)
    {
        //  @see http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/client.html
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", task.getClusterName())
                .build();
        TransportClient client = TransportClient.builder().settings(settings).build();
        List<NodeAddressTask> nodes = task.getNodes();
        for (NodeAddressTask node : nodes) {
            try {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(node.getHost()), node.getPort()));
            } catch (UnknownHostException e) {
                Throwables.propagate(e);
            }
        }
        return client;
    }
}
