# Elasticsearch output plugin for Embulk

## Overview

* **Plugin type**: output
* **Rollback supported**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **nodes**: list of nodes. nodes are pairs of host and port (list, required)
- **cluster_name**: name of the cluster (string, default is "elasticsearch")
- **index**: index name (string, required)
- **index_type**: index type (string, required)
- **id**: document id column (string, default is null)
- **bulk_actions**: Sets when to flush a new bulk request based on the number of actions currently added. (int, default is 1000)
- **bulk_size**: Sets when to flush a new bulk request based on the size of actions currently added. (long, default is 5242880)
- **concurrent_requests**: concurrent_requests (int, default is 5)

## Example

### Elasticsearch 2.x

```yaml
out:
  type: elasticsearch_2x
  nodes:
  - {host: localhost, port: 9300}
  index: <index name>
  index_type: <index type>
```

### Elasticsearch 1.x

```yaml
out:
  type: elasticsearch_1x
  nodes:
  - {host: localhost, port: 9300}
  index: <index name>
  index_type: <index type>
```

## Build

```
$ ./gradlew gem
```
