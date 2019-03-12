This plugin allows presto to interact with [iceberg][iceberg]  tables.

[iceberg]: https://github.com/Netflix/iceberg

## Status

Currently this plugin supports create, CTAS, drop and reading from iceberg table. Support for other DDL operation will be added soon.


## How to configure

The plugin extends from hive-plugin so it needs metastore configs and s3 configs if you use s3. In addition the current implementation relies
on [HiveTables][HiveTables]  implementation which relies on `metastore.thrift.uris` and `hive.metastore.warehouse.dir` values from the hive-site.xml. 

You can look at the sample configuration under `presto-main/etc/catalog/iceberg.properties`

[HiveTables]: https://github.com/Netflix/iceberg/tree/master/hive/src/main/java/com/netflix/iceberg/hive

### How to create an iceberg table
Just like a hive table, the only difference is you will specify the iceberg catalog instead of a hive catalog.

## Unpartitioned Tables
``` sql
create table iceberg.testdb.sample (
    i int, 
    s varchar
);
```
## Partitioned Tables
Currently we only support identity partitions so there is no difference in hive vs iceberg syntax. Just like unpartitioned table you must specify iceberg catalog to create iceberg tables.

``` sql
create table iceberg.testdb.sample_partitioned (
    b boolean,
    dateint integer,
    l bigint,
    f real,
    d double,
    de decimal(12,2),
    dt date,
    ts timestamp,
    s varchar,
    bi varbinary
 )
WITH (partitioned_by = ARRAY['dateint', 's']);
```

## Insert and Select
There is no uniqueness in these cases just select and insert like you would to any hive table. The big difference is you should be able
to add,drop and rename columns without any issues.

## Migrating existing tables
This plugin can read/write to hive tables that have been migrated to iceberg. Currently there is no presto support to migrate hive
tables to presto so you will either need to use icerberg API or use spark.

## Hidden columns
Iceberg supports $snapshot_id and $snapshot_timestamp_ms as hidden columns. These columns allows users to query an old version of
the table. In addition it also supports $partitions for hive compatibility.

## Still to do
Support for delete from.
Support for hidden partitioning.
Support for time type.
Bucketing support.
Iceberg table properties.
Create table like support.
Support for column level comments.
Remove dependency on presto-hive plugin , extract the metastore classes and security module out.
Explore if we can support/extend presto sql dialect so users can have "migrate table", "rollback table to snapshot" equivalents in presto.


