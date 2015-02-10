# Elasticsearch output plugin for Embulk

## Overview

* **Plugin type**: output
* **Rollback supported**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

```yaml
out:
  type: elasticsearch
  cluster: <cluster name>
  index_name: <your index>
  index_type: <your index type>
```

## Build

```
$ ./gradlew gem
```