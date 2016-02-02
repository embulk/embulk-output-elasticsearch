package org.embulk.output.elasticsearch.v2;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Throwables;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.embulk.config.ConfigException;
import org.embulk.output.elasticsearch.ElasticsearchOutputPlugin.NodeAddressTask;
import org.embulk.output.elasticsearch.ElasticsearchOutputPlugin.PluginTask;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class ElasticsearchClient
        implements org.embulk.output.elasticsearch.ElasticsearchClient
{
    public static class Builder
            extends org.embulk.output.elasticsearch.ElasticsearchClient.Builder
    {
        public org.embulk.output.elasticsearch.ElasticsearchClient build()
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
        try {
            if (!isExistsAlias(aliasName, client)) {
                client.admin().indices().prepareAliases()
                        .addAlias(newIndexName, aliasName)
                        .execute().actionGet();
                log.info(String.format("Assigned alias[%s] to index[%s]", aliasName, newIndexName));
            }
            else {
                List<String> oldIndices = getIndexByAlias(aliasName, client);
                client.admin().indices().prepareAliases()
                        .removeAlias(oldIndices.toArray(new String[oldIndices.size()]), aliasName)
                        .addAlias(newIndexName, aliasName)
                        .execute().actionGet();
                log.info(String.format("Reassigned alias[%s] from index%s to index[%s]", aliasName, oldIndices, newIndexName));
                for (String index : oldIndices) {
                    deleteIndex(index, client);
                }
            }
        }
        catch (IndexNotFoundException | InvalidAliasNameException e) {
            throw new ConfigException(e);
        }
    }

    boolean isExistsAlias(String aliasName, Client client)
    {
        return client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().getMetaData().hasAlias(aliasName);
    }

    List<String> getIndexByAlias(String aliasName, Client client)
    {
        ImmutableOpenMap<String, List<AliasMetaData>> map = client.admin().indices().getAliases(new GetAliasesRequest(aliasName)).actionGet().getAliases();
        List<String> indices = new ArrayList<>();
        for (ObjectObjectCursor<String, List<AliasMetaData>> c : map) {
            indices.add(c.key);
        }

        return indices;
    }

    void deleteIndex(String indexName, Client client)
    {
        client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
        log.info(String.format("Deleted Index [%s]", indexName));
    }

    public boolean isExistsIndex(String indexName)
    {
        return client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().getMetaData().hasIndex(indexName);
    }

    public boolean isAlias(String aliasName)
    {
        AliasOrIndex aliasOrIndex = client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().getMetaData().getAliasAndIndexLookup().get(aliasName);
        return aliasOrIndex != null && aliasOrIndex.isAlias();
    }

    @Override
    public void close()
    {
        client.close();
    }

}
