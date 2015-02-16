# Elasticsearch output plugin for Embulk

## Overview

* **Plugin type**: output
* **Rollback supported**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **cluster**: cluster name (string, default: 'elasticsearch')
- **index_name**: index name (string, required)
- **index_type**: index type (string, required)

## Example

```yaml
out:
  type: elasticsearch
  cluster: elasticsearch
  index_name: embulk
  index_type: embulk
```

## Build

```
$ ./gradlew gem
```