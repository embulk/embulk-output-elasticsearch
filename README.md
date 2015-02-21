# Elasticsearch output plugin for Embulk

## Overview

* **Plugin type**: output
* **Rollback supported**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **transport_addresses**: list of pairs of address and port (list, required)
- **index_name**: index name (string, required)
- **index_type**: index type (string, required)

## Example

```yaml
out:
  type: elasticsearch
  transport_addresses:
  - {hostname: localhost, port: 9300}
  index_name: embulk
  index_type: embulk
```

## Build

```
$ ./gradlew gem
```