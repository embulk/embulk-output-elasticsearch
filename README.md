# OpenSearch output plugin for Embulk

- Use [opensearch-java](https://github.com/opensearch-project/opensearch-java) client
- Test in [OpenSearch Docker](https://hub.docker.com/r/opensearchproject/opensearch) container
- [Support Embulk v0.11](https://www.embulk.org/articles/2021/04/27/changes-in-v0.11.html)

## Overview

* **Plugin type**: output
* **Rollback supported**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Installation

https://github.com/users/calorie/packages/rubygems/package/embulk-output-opensearch

Install via Gemfile:

```ruby
source "https://rubygems.pkg.github.com/calorie" do
  gem "embulk-output-opensearch", "1.0.0"
end
```

## Configuration

- **mode**: "insert" or "replace". See below(string, optional, default is insert)
- **nodes**: list of nodes. nodes are pairs of host and port (list, required)
  - NOTE: This plugin uses HTTP/REST Clients and uses TCP:9200 as a default. TCP:9300 is usually used for Transport Client.
- **use_ssl** Use SSL encryption (boolean, default is false)
- **auth_method** (string, default is 'none') 'none'/'basic'. See also [Authentication](#authentication).
- **user** Username for basic authentication (string, default is null)
- **password** Password for above user (string, default is null)
- **index**: index name (string, required)
- **id**: document id column (string, default is null)
- **bulk_actions**: Sets when to flush a new bulk request based on the number of actions currently added. (int, default is 1000)
- **bulk_size**: Sets when to flush a new bulk request based on the size of actions currently added. (long, default is 5242880)
- **fill_null_for_empty_column**: Fill null value when column value is empty (boolean, optional, default is false)
- **maximum_retries** Number of maximam retry times (int, optional, default is 7)
- **initial_retry_interval_millis** Initial interval between retries in milliseconds (int, optional, default is 1000)
- **maximum_retry_interval_millis** Maximum interval between retries in milliseconds (int, optional, default is 120000)
- **timeout_millis** timeout in milliseconds for each HTTP request(int, optional, default is 60000)
- **connect_timeout_millis** connection timeout in milliseconds for HTTP client(int, optional, default is 60000)
- **max_snapshot_waiting_secs** maximam waiting time in second when snapshot is just creating before delete index. works when `mode: replace` (int, optional, default is 1800)
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
  type: opensearch
  mode: replace
  nodes:
  - {host: localhost, port: 9200}
  index: <alias name> # plugin generates index name like <index>_%Y%m%d-%H%M%S
```

### Authentication

This plugin supports Basic authentication.
'Security' also supports LDAP and Active Directory. This plugin doesn't supports these auth methods.

```yaml
use_ssl: true
auth_method: basic
user: <username>
password: <password>
```

## Example

```yaml
out:
  type: opensearch
  mode: insert
  nodes:
  - {host: localhost, port: 9200}
  index: <index name>
```

## Test

Firstly install Docker and Docker compose then `docker compose up opensearch`,
so that an ES server will be locally launched then you can run tests with `docker compose run --rm java ./gradlew test`.

```sh
docker compose up opensearch
docker compose run --rm java ./gradlew test  # -t to watch change of files and rebuild continuously
```

For Maintainers
----------------

### Release

Modify `version` in `build.gradle` at a detached commit, and then tag the commit with an annotation.

```sh
git checkout --detach main
# (Edit: Remove "-SNAPSHOT" in "version" in build.gradle.)
git add build.gradle
git commit -m "Release vX.Y.Z"
git tag -a vX.Y.Z
# (Edit: Write a tag annotation in the changelog format.)
```

See [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) for the changelog format. We adopt a part of it for Git's tag annotation like below.

```
## [X.Y.Z] - YYYY-MM-DD
### Added
- Added a feature.
### Changed
- Changed something.
### Fixed
- Fixed a bug.
```

Push the annotated tag, then. It triggers a release operation on GitHub Actions after approval.

```sh
git push -u origin vX.Y.Z
```
