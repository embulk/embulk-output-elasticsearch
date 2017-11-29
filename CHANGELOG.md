## 0.4.5 - 2017-11-29
* [new feature] Add "fill_null_for_empty_column" option and allow insert null value when column is empty [#47](https://github.com/embulk/embulk-output-elasticsearch/pull/47) Thanks! @kfitzgerald

## 0.4.4 - 2017-06-16

* [maintenance] Improve retry logic - Create RetryHelper instance only at sendRequest() method [#41](https://github.com/muga/embulk-output-elasticsearch/pull/41)

## 0.4.3 - 2017-06-12

* [maintenance] Improve exception handling [#38](https://github.com/muga/embulk-output-elasticsearch/pull/38)
* [maintenance] Fix ElasticsearchRecordBuffer to call retryHelper.close() [#39](https://github.com/muga/embulk-output-elasticsearch/pull/39)

## 0.4.2 - 2017-05-31

* [maintenance] Update embulk-base-restclient to fix ArrayIndexOutOfBoundsException [#37](https://github.com/muga/embulk-output-elasticsearch/pull/37)

## 0.4.1 - 2017-04-21

* [maintenance] Check snapshot progress status before delete index [#36](https://github.com/muga/embulk-output-elasticsearch/pull/36)

## 0.4.0 - 2017-03-28

* [new feature] Support multiple Elasticsearch version [#32](https://github.com/muga/embulk-output-elasticsearch/pull/32)
* [new feature] Support SSL and Basic authentication including 'Security'(formally 'Shield') [#33](https://github.com/muga/embulk-output-elasticsearch/pull/33)
* [maintenance] Improve export logic [#34](https://github.com/muga/embulk-output-elasticsearch/pull/34)

## 0.3.1 - 2016-06-21

* [maintenance] Update Elasticsearch client to 2.3.3 [#25](https://github.com/muga/embulk-output-elasticsearch/pull/25)

## 0.3.0 - 2016-02-22

* [maintenance] Upgrade Embulk v08 [#21](https://github.com/muga/embulk-output-elasticsearch/pull/21)

## 0.2.1 - 2016-02-05

* [maintenance] Fix bug. Force to fail jobs if nodes down while executing [#19](https://github.com/muga/embulk-output-elasticsearch/pull/19)

## 0.2.0 - 2016-01-26

* [new feature] Support Elasticsearch 2.x [#12](https://github.com/muga/embulk-output-elasticsearch/pull/12)
* [new feature] Added replace mode [#15](https://github.com/muga/embulk-output-elasticsearch/pull/15)
* [maintenance] Fix id param's behavior [#14](https://github.com/muga/embulk-output-elasticsearch/pull/14)
* [maintenance] Added unit tests [#17](https://github.com/muga/embulk-output-elasticsearch/pull/17)
* [maintenance] Upgraded Embulk to v0.7.7

## 0.1.8 - 2015-08-19

* [maintenance] Upgraded Embulk to v0.7.0
* [maintenance] Upgraded Elasticsearch to v1.5.2

## 0.1.7 - 2015-05-09

* [maintenance] Fixed handling null value [#10](https://github.com/muga/embulk-output-elasticsearch/pull/10)

## 0.1.6 - 2015-04-14

* [new feature] Added bulk_size parameter [#8](https://github.com/muga/embulk-output-elasticsearch/pull/8)

## 0.1.5 - 2015-03-26

* [new feature] Added cluster_name parameter [#7](https://github.com/muga/embulk-output-elasticsearch/pull/7)

## 0.1.4 - 2015-03-19

* [maintenance] Fixed parameter names index_name to index, doc_id_column to id. [#5](https://github.com/muga/embulk-output-elasticsearch/pull/5)
* [maintenance] Fixed typo at parameter [#6](https://github.com/muga/embulk-output-elasticsearch/pull/6)

## 0.1.3 - 2015-02-25

* [new feature] Supported timestamp column [#4](https://github.com/muga/embulk-output-elasticsearch/pull/4)

## 0.1.2 - 2015-02-24

## 0.1.1 - 2015-02-16

## 0.1.0 - 2015-02-16
