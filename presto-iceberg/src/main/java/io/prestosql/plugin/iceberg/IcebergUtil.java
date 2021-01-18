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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.metacat.MetacatIcebergCatalog;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle.ColumnType;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import javax.inject.Inject;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.collect.Lists.reverse;
import static com.google.shaded.shaded.common.collect.Maps.uniqueIndex;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_SNAPSHOT_ID;
import static io.prestosql.plugin.iceberg.TypeConverter.toPrestoType;
import static java.lang.String.format;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

final class IcebergUtil
{
    private static final Pattern SIMPLE_NAME = Pattern.compile("[a-z][a-z0-9]*");

    // private IcebergUtil() {} anjali remove

    public static final String NETFLIX_METACAT_HOST = "netflix.metacat.host";
    public static final String NETFLIX_WAREHOUSE_DIR = "hive.metastore.warehouse.dir";
    public static final String APP_NAME = "presto-" + System.getenv("stack");

    private final IcebergConfig config;

    @Inject
    public IcebergUtil(IcebergConfig config)
    {
        this.config = config;
    }

    public static boolean isIcebergTable(io.prestosql.plugin.hive.metastore.Table table)
    {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(table.getParameters().get(TABLE_TYPE_PROP));
    }

    public Optional<Table> getIcebergTable(HiveMetastore metastore, HdfsEnvironment hdfsEnvironment, ConnectorSession session, SchemaTableName table)
    {
        // Configuration configuration = new Configuration(false); anjali this is not working
        // anjali Major hack David's commit removes this tmp code, adding it back to get things to work. I was getting s3n file system not known/found error
        // on insert into iceberg table
        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session, table.getSchemaName()),
                new Path("file:///tmp"));
        try {
            return Optional.of(getCatalog(configuration).loadTable(toTableIdentifier(config, table.getSchemaName(), table.getTableName())));
        }
        catch (Exception e) {
            return Optional.empty();
        }
    }

    public static long resolveSnapshotId(Table table, long snapshotId)
    {
        if (table.snapshot(snapshotId) != null) {
            return snapshotId;
        }

        return reverse(table.history()).stream()
            .filter(entry -> entry.timestampMillis() <= snapshotId)
            .map(HistoryEntry::snapshotId)
            .findFirst()
            .orElseThrow(() -> new PrestoException(ICEBERG_INVALID_SNAPSHOT_ID, format("Invalid snapshot [%s] for table: %s", snapshotId, table)));
    }

    public MetacatIcebergCatalog getCatalog(Configuration configuration)
    {
        configuration.set(NETFLIX_METACAT_HOST, config.getMetastoreRestEndpoint());
        configuration.set(NETFLIX_WAREHOUSE_DIR, config.getMetastoreWarehoseDir());
        // anjali productize this
        configuration.set("fs.s3n.impl", "io.prestosql.plugin.hive.s3.PrestoS3FileSystem");
        return new MetacatIcebergCatalog(configuration, APP_NAME);
    }

    public static List<IcebergColumnHandle> getColumns(Schema schema, PartitionSpec spec, TypeManager typeManager)
    {
        // Iceberg may or may not store identity columns in data file and the identity transformations have the same name as data column.
        // So we remove the identity columns from the set of regular columns which does not work with some of Presto's validation.

        List<PartitionField> partitionFields = ImmutableList.copyOf(getPartitions(spec, false).keySet());
        Map<String, PartitionField> partitionColumnNames = uniqueIndex(partitionFields, PartitionField::name);

        int columnIndex = 0;
        ImmutableList.Builder<IcebergColumnHandle> builder = ImmutableList.builder();

        for (Types.NestedField column : schema.columns()) {
            Type type = column.type();
            ColumnType columnType = REGULAR;
            if (partitionColumnNames.containsKey(column.name())) {
                PartitionField partitionField = partitionColumnNames.get(column.name());
                Type sourceType = schema.findType(partitionField.sourceId());
                type = partitionField.transform().getResultType(sourceType);
                columnType = PARTITION_KEY;
            }
            IcebergColumnHandle columnHandle = new IcebergColumnHandle(
                    column.fieldId(),
                    column.name(),
                    toPrestoType(column.type(), typeManager),
                    Optional.ofNullable(column.doc()));
            builder.add(columnHandle);
        }

        return builder.build();
    }

    public static Map<PartitionField, Integer> getIdentityPartitions(PartitionSpec partitionSpec)
    {
        // TODO: expose transform information in Iceberg library
        ImmutableMap.Builder<PartitionField, Integer> columns = ImmutableMap.builder();
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            PartitionField field = partitionSpec.fields().get(i);
            if (field.transform().toString().equals("identity")) {
                columns.put(field, i);
            }
        }
        return columns.build();
    }

    public static Map<PartitionField, Integer> getPartitions(PartitionSpec partitionSpec, boolean identityPartitionsOnly)
    {
        // TODO: expose transform information in Iceberg library
        ImmutableMap.Builder<PartitionField, Integer> columns = ImmutableMap.builder();
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            PartitionField field = partitionSpec.fields().get(i);
            if (identityPartitionsOnly == false || (identityPartitionsOnly && field.transform().toString().equals("identity"))) {
                columns.put(field, i);
            }
        }
        return columns.build();
    }

    public static String getDataPath(String location)
    {
        if (!location.endsWith("/")) {
            location += "/";
        }
        return location + "data";
    }

    public static FileFormat getFileFormat(Table table)
    {
        return FileFormat.valueOf(table.properties()
                .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .toUpperCase(Locale.ENGLISH));
    }

    public static Optional<String> getTableComment(Table table)
    {
        return Optional.ofNullable(table.properties().get(TABLE_COMMENT));
    }

    private static String quotedTableName(SchemaTableName name)
    {
        return quotedName(name.getSchemaName()) + "." + quotedName(name.getTableName());
    }

    private static String quotedName(String name)
    {
        if (SIMPLE_NAME.matcher(name).matches()) {
            return name;
        }
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    public static TableIdentifier toTableIdentifier(IcebergConfig icebergConfig, String db, String tableName)
    {
        Namespace namespace = Namespace.of(icebergConfig.getMetacatCatalogName(), db);
        return TableIdentifier.of(namespace, tableName);
    }
}
