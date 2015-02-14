# Elasticsearch output plugin for Embulk

## Overview

* **Plugin type**: output
* **Rollback supported**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **cluster**: description (string, required)
- **index_name**: description (integer, default: default-value)
- **index_type**: description (integer, default: default-value)

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