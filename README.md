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
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## Test

```
$ ./gradlew test  # -t to watch change of files and rebuild continuously
```

To run unit tests, we need to configure the following environment variables.

When environment variables are not set, skip almost test cases.

```
ES_HOST
ES_PORT(optional, if needed, default: 9300)
ES_INDEX
ES_INDEX_TYPE
```

If you're using Mac OS X El Capitan and GUI Applications(IDE), like as follows.
```
$ vi ~/Library/LaunchAgents/environment.plist
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>my.startup</string>
  <key>ProgramArguments</key>
  <array>
    <string>sh</string>
    <string>-c</string>
    <string>
      launchctl setenv ES_HOST example.com
      launchctl setenv ES_PORT 9300
      launchctl setenv ES_INDEX embulk
      launchctl setenv ES_INDEX_TYPE embulk
    </string>
  </array>
  <key>RunAtLoad</key>
  <true/>
</dict>
</plist>

$ launchctl load ~/Library/LaunchAgents/environment.plist
$ launchctl getenv ES_INDEX //try to get value.

Then start your applications.
```