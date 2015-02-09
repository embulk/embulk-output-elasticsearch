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
  host: <elasticsearch host>
  port: <elasticsearch port>
  index: <your index>
  index_type: <your index type>
```

## Build

```
$ ./gradlew gem
```