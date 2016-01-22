# Elasticsearch output plugin for Embulk

## Overview

* **Plugin type**: output
* **Rollback supported**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **mode**: "insert" or "replace". See below(string, optional, default is insert)
- **nodes**: list of nodes. nodes are pairs of host and port (list, required)
- **cluster_name**: name of the cluster (string, default is "elasticsearch")
- **index**: index name (string, required)
- **index_type**: index type (string, required)
- **id**: document id column (string, default is null)
- **bulk_actions**: Sets when to flush a new bulk request based on the number of actions currently added. (int, default is 1000)
- **bulk_size**: Sets when to flush a new bulk request based on the size of actions currently added. (long, default is 5242880)
- **concurrent_requests**: concurrent_requests (int, default is 5)

### Modes

#### insert:

default.
This mode writes data to existing index.

#### replace:

1. Create new temporary index 
2. Insert data into the new index
3. replace the alias with the new index. If alias doesn't exists, plugin will create new alias.
4. Delete existing (old) index if exists

Index should not exists with the same name as the alias

```yaml
out:
  type: elasticsearch
  mode: replace
  nodes:
  - {host: localhost, port: 9300}
  index: <alias name> # plugin generates index name like <index>_%Y%m%d-%H%M%S 
  index_type: <index type>
```

## Example

```yaml
out:
  type: elasticsearch
  mode: insert
  nodes:
  - {host: localhost, port: 9300}
  index: <index name>
  index_type: <index type>
```

## Build

```
$ ./gradlew gem
```
