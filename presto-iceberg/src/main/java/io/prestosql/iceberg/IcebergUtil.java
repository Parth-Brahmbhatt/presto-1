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
package io.prestosql.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.prestosql.iceberg.type.TypeConveter;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hive.HiveTables;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.stream.Collectors.toMap;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

class IcebergUtil
{
    public static final String ICEBERG_PROPERTY_NAME = "table_type";
    public static final String ICEBERG_PROPERTY_VALUE = "iceberg";
    private static final TypeTranslator hiveTypeTranslator = new HiveTypeTranslator();
    private static final String PATH_SEPERATOR = "/";
    public static final String DATA_DIR_NAME = "data";

    @Inject
    public IcebergUtil()
    {
    }

    public final boolean isIcebergTable(io.prestosql.plugin.hive.metastore.Table table)
    {
        Map<String, String> parameters = table.getParameters();
        return parameters != null && !parameters.isEmpty() && ICEBERG_PROPERTY_VALUE.equalsIgnoreCase(parameters.get(ICEBERG_PROPERTY_NAME));
    }

    public Table getIcebergTable(String database, String tableName, Configuration configuration)
    {
        return getTable(configuration).load(database, tableName);
    }

    public HiveTables getTable(Configuration configuration)
    {
        return new HiveTables(configuration);
    }

    public List<HiveColumnHandle> getColumns(Schema schema, PartitionSpec spec, TypeManager typeManager)
    {
        List<Types.NestedField> columns = schema.columns();
        int columnIndex = 0;
        ImmutableList.Builder builder = ImmutableList.builder();
        List<PartitionField> partitionFields = getIdentityPartitions(spec).entrySet().stream().map(e -> e.getKey()).collect(Collectors.toList());
        Map<String, PartitionField> partitionColumnNames = partitionFields.stream().collect(toMap(PartitionField::name, Function.identity()));
        // Iceberg may or may not store identity columns in data file and the identity transformations have the same name as data column.
        // So we remove the identity columns from the set of regular columns which does not work with some of presto validation.

        for (Types.NestedField column : columns) {
            Type type = column.type();
            HiveColumnHandle.ColumnType columnType = REGULAR;
            if (partitionColumnNames.containsKey(column.name())) {
                PartitionField partitionField = partitionColumnNames.get(column.name());
                Type sourceType = schema.findType(partitionField.sourceId());
                type = partitionField.transform().getResultType(sourceType);
                columnType = PARTITION_KEY;
            }
            io.prestosql.spi.type.Type prestoType = TypeConveter.convert(type, typeManager);
            HiveType hiveType = HiveType.toHiveType(hiveTypeTranslator, coerceForHive(prestoType));
            HiveColumnHandle columnHandle = new HiveColumnHandle(column.name(), hiveType, prestoType.getTypeSignature(), columnIndex++, columnType, Optional.empty());
            builder.add(columnHandle);
        }

        return builder.build();
    }

    public io.prestosql.spi.type.Type coerceForHive(io.prestosql.spi.type.Type prestoType)
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

    public final TableScan getTableScan(ConnectorSession session, TupleDomain<HiveColumnHandle> predicates, Long id, Table icebergTable)
    {
        Expression expression = ExpressionConverter.toIceberg(predicates, session);
        TableScan tableScan = icebergTable.newScan().filter(expression);
        if (id != null) {
            if (isSnapshot(icebergTable, id)) {
                tableScan = tableScan.useSnapshot(id);
            }
            else {
                tableScan = tableScan.asOfTime(id);
            }
        }
        return tableScan;
    }

    private boolean isSnapshot(Table icebergTable, Long id)
    {
        Iterator<Snapshot> snapshots = icebergTable.snapshots().iterator();
        while (snapshots != null && snapshots.hasNext()) {
            if (snapshots.next().snapshotId() == id) {
                return true;
            }
        }
        return false;
    }

    public Long getPredicateValue(TupleDomain<HiveColumnHandle> predicates, String columnName)
    {
        if (predicates.isNone() || predicates.isAll()) {
            return null;
        }

        return predicates.getDomains().map(hiveColumnHandleDomainMap -> {
            List<Domain> snapShotDomains = hiveColumnHandleDomainMap.entrySet().stream()
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
