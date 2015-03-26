# Elasticsearch output plugin for Embulk

## Overview

* **Plugin type**: output
* **Rollback supported**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **nodes**: list of nodes. nodes are pairs of host and port (list, required)
- **cluster_name**: name of the cluster (string, default is "elasticsearch")
- **index_name**: index name (string, required)
- **index_type**: index type (string, required)
- **doc_id_column**: document id column (string, default is null)
- **bulk_actions**: bulk_actions (int, default is 1000)
- **concurrent_requests**: concurrent_requests (int, default is 5)

## Example

```yaml
out:
  type: elasticsearch
  node:
  - {host: localhost, port: 9300}
  index_name: <index name>
  index_type: <index type>
```

## Build

```
$ ./gradlew gem
```
