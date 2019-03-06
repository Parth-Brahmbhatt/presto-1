/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.iceberg.type.TypeConveter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.PartitionField;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableScan;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.hive.HiveTables;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static java.util.stream.Collectors.toMap;

class IcebergUtil
{
    public static final String ICEBERG_PROPERTY_NAME = "table_type";
    public static final String ICEBERG_PROPERTY_VALUE = "iceberg";
    private static final TypeTranslator hiveTypeTranslator = new HiveTypeTranslator();
    private static final String PATH_SEPERATOR = "/";
    public static final String DATA_DIR_NAME = "data";
    public static final String SNAPSHOT_ID = "$snapshot_id";
    public static final String SNAPSHOT_TIMESTAMP_MS = "$snapshot_timestamp_ms";


    @Inject
    public IcebergUtil()
    {
    }

    public final boolean isIcebergTable(com.facebook.presto.hive.metastore.Table table)
    {
        final Map<String, String> parameters = table.getParameters();
        return parameters != null && !parameters.isEmpty() && ICEBERG_PROPERTY_VALUE.equalsIgnoreCase(parameters.get(ICEBERG_PROPERTY_NAME));
    }

    public Table getIcebergTable(String database, String tableName, Configuration configuration)
    {
        return getHiveTables(configuration).load(database, tableName);
    }

    public HiveTables getHiveTables(Configuration configuration)
    {
        return new HiveTables(configuration);
    }

    public List<HiveColumnHandle> getColumns(Schema schema, PartitionSpec spec, TypeManager typeManager)
    {
        final List<Types.NestedField> columns = schema.columns();
        int columnIndex = 0;
        ImmutableList.Builder builder = ImmutableList.builder();
        final List<PartitionField> partitionFields = getIdentityPartitions(spec).entrySet().stream().map(e -> e.getKey()).collect(Collectors.toList());
        final Map<String, PartitionField> partitionColumnNames = partitionFields.stream().collect(toMap(PartitionField::name, Function.identity()));
        // Iceberg may or may not store identity columns in data file and the identity transformations have the same name as data column.
        // So we remove the identity columns from the set of regular columns which does not work with some of presto validation.

        for (Types.NestedField column : columns) {
            Type type = column.type();
            HiveColumnHandle.ColumnType columnType = REGULAR;
            if (partitionColumnNames.containsKey(column.name())) {
                final PartitionField partitionField = partitionColumnNames.get(column.name());
                Type sourceType = schema.findType(partitionField.sourceId());
                type = partitionField.transform().getResultType(sourceType);
                columnType = PARTITION_KEY;
            }
            final com.facebook.presto.spi.type.Type prestoType = TypeConveter.convert(type, typeManager);
            final HiveType hiveType = HiveType.toHiveType(hiveTypeTranslator, coerceForHive(prestoType));
            final HiveColumnHandle columnHandle = new HiveColumnHandle(column.name(), hiveType, prestoType.getTypeSignature(), columnIndex++, columnType, Optional.empty());
            builder.add(columnHandle);
        }

        builder.add(new HiveColumnHandle(SNAPSHOT_ID, HIVE_LONG, BIGINT.getTypeSignature(), columnIndex++, SYNTHESIZED, Optional.empty()));
        builder.add(new HiveColumnHandle(SNAPSHOT_TIMESTAMP_MS, HIVE_LONG, BIGINT.getTypeSignature(), columnIndex++, SYNTHESIZED, Optional.empty()));

        return builder.build();
    }

    public com.facebook.presto.spi.type.Type coerceForHive(com.facebook.presto.spi.type.Type prestoType)
    {
        if (prestoType.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return TIMESTAMP;
        }
        return prestoType;
    }

    public static final Map<PartitionField, Integer> getIdentityPartitions(PartitionSpec partitionSpec)
    {
        //TODO We are only treating identity column as partition columns as we do not want all other columns to be projectable or filterable.
        // Identity class is not public so no way to really identify if a transformation is identity transformation or not other than checking toString as of now.
        // Need to make changes to iceberg so we can identify transform in a better way.
        return IntStream.range(0, partitionSpec.fields().size())
                .boxed()
                .collect(toMap(partitionSpec.fields()::get, i -> i))
                .entrySet()
                .stream()
                .filter(e -> e.getKey().transform().toString().equals("identity"))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public String getDataPath(String icebergLocation)
    {
        return icebergLocation.endsWith(PATH_SEPERATOR) ? icebergLocation + DATA_DIR_NAME : icebergLocation + PATH_SEPERATOR + DATA_DIR_NAME;
    }

    public FileFormat getFileFormat(Table table)
    {
        return FileFormat.valueOf(table.properties()
                .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .toUpperCase(Locale.ENGLISH));
    }

    public final TableScan getTableScan(ConnectorSession session, TupleDomain<HiveColumnHandle> predicates, Long snapshotId, Long snapshotTimestamp, Table icebergTable)
    {
        if (snapshotId != null && snapshotTimestamp != null) {
            throw new IllegalArgumentException(String.format("Either specify a predicate on %s or %s but not both", SNAPSHOT_ID, SNAPSHOT_TIMESTAMP_MS));
        }

        final Expression expression = ExpressionConverter.toIceberg(predicates, session);
        TableScan tableScan = icebergTable.newScan().filter(expression);

        if (snapshotId != null) {
            tableScan = tableScan.useSnapshot(snapshotId);
        }
        else if (snapshotTimestamp != null) {
            tableScan = tableScan.asOfTime(snapshotTimestamp);
        }
        return tableScan;
    }

    public final Long getPredicateValue(TupleDomain<HiveColumnHandle> predicates, String columnName)
    {
        if (predicates.isNone() || predicates.isAll()) {
            return null;
        }

        return predicates.getDomains().map(hiveColumnHandleDomainMap -> {
            final List<Domain> snapShotDomains = hiveColumnHandleDomainMap.entrySet().stream()
                    .filter(hiveColumnHandleDomainEntry -> hiveColumnHandleDomainEntry.getKey().getName().equals(columnName))
                    .map(hiveColumnHandleDomainEntry -> hiveColumnHandleDomainEntry.getValue())
                    .collect(Collectors.toList());

            if (snapShotDomains.isEmpty()) {
                return null;
            }

            if (snapShotDomains.size() > 1 || !snapShotDomains.get(0).isSingleValue()) {
                throw new IllegalArgumentException(String.format("Only %s = value check is allowed on column = %s", columnName, columnName));
            }

            return (Long) snapShotDomains.get(0).getSingleValue();
        }).orElse(null);
    }
}
