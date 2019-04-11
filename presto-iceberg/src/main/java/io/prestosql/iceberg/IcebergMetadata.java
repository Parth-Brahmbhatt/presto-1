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
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.prestosql.iceberg.type.TypeConveter;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveWrittenPartitions;
import io.prestosql.plugin.hive.LocationHandle;
import io.prestosql.plugin.hive.LocationService;
import io.prestosql.plugin.hive.TransactionalMetadata;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutResult;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hive.HiveTables;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.plugin.hive.HiveColumnHandle.updateRowIdHandle;
import static io.prestosql.plugin.hive.HiveTableProperties.getPartitionedBy;
import static io.prestosql.plugin.hive.HiveUtil.schemaTableName;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class IcebergMetadata
        implements ConnectorMetadata, TransactionalMetadata
{
    private static final String SCHEMA_PROPERTY = "schema";
    private static final String PARTITION_SPEC_PROPERTY = "partition_spec";
    private static final String TABLE_PROPERTIES = "table_properties";
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final SemiTransactionalHiveMetastore metastore;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private Transaction transaction;
    private IcebergUtil icebergUtil;
    private final LocationService locationService;

    IcebergMetadata(
            SemiTransactionalHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> jsonCodec,
            IcebergUtil icebergUtil,
            LocationService locationService)
    {
        this.hdfsEnvironment = hdfsEnvironment;
        this.typeManager = typeManager;
        this.metastore = metastore;
        this.jsonCodec = jsonCodec;
        this.icebergUtil = icebergUtil;
        this.locationService = locationService;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases();
    }

    @Override
    public IcebergTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableHandle tableHandle = IcebergTableHandle.parse(tableName.getTableName(), tableName.getSchemaName());
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table.isPresent()) {
            if (icebergUtil.isIcebergTable(table.get())) {
                return tableHandle;
            }
            else {
                throw new RuntimeException(String.format("%s is not an iceberg table please query using hive catalog", tableName));
            }
        }
        return null;
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableHandle sourceTableHandle = IcebergTableHandle.parse(tableName.getTableName(), tableName.getSchemaName());

        if (sourceTableHandle.getTableType() == TableType.PARTITIONS) {
            return Optional.of(new PartitionTable(sourceTableHandle, session, icebergUtil, getConfiguration(session, tableName.getSchemaName()), typeManager));
        }
        else {
            return Optional.empty();
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
            ConnectorTableHandle tbl,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        IcebergTableHandle tableHandle = (IcebergTableHandle) tbl;
        Map<String, HiveColumnHandle> nameToHiveColumnHandleMap = desiredColumns
                .map(cols -> cols.stream().map(HiveColumnHandle.class::cast)
                        .collect(toMap(HiveColumnHandle::getName, identity())))
                .orElse(emptyMap());

        TupleDomain<HiveColumnHandle> predicates = constraint.getSummary().getDomains()
                .map(m -> m.entrySet().stream().collect(Collectors.toMap((x) -> HiveColumnHandle.class.cast(x.getKey()), Map.Entry::getValue)))
                .map(m -> TupleDomain.withColumnDomains(m)).orElse(TupleDomain.none());

        // TODO The only predicates that will be applied by engines are the ones that we return in ConnectorTableLayoutResult
        // so when we start supporting non identity partitions we will either need to perform a scan here and return the residual from scan
        // or we will need to return both partition and non partition predicates which would brake Delete as we do not implement beginDelete
        // and endDelete but instead we implement metadataDelete as the actual row level delete is not supported by iceberg.

        TupleDomain<ColumnHandle> nonPartitionPredicate = constraint.getSummary().getDomains()
                .map(m -> m.entrySet().stream().filter(e -> ((HiveColumnHandle) e.getKey()).getColumnType() != HiveColumnHandle.ColumnType.PARTITION_KEY)
                        .collect(Collectors.toMap((x) -> x.getKey(), Map.Entry::getValue)))
                .map(m -> TupleDomain.withColumnDomains(m)).orElse(TupleDomain.none());

        // TODO Optimization opportunity if we provide proper IcebergTableLayoutHandle so we do not have to keep loading iceberg table from the metadata.
        IcebergTableLayoutHandle icebergTableLayoutHandle = new IcebergTableLayoutHandle(tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getAtId(),
                predicates,
                nameToHiveColumnHandleMap);
        return ImmutableList.of(new ConnectorTableLayoutResult(new ConnectorTableLayout(icebergTableLayoutHandle), nonPartitionPredicate));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        IcebergTableHandle tbl = (IcebergTableHandle) table;
        return getTableMetadata(tbl.getSchemaName(), tbl.getTableName(), session);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchema)
    {
        List<String> schemas = optionalSchema.<List<String>>map(ImmutableList::of)
                .orElseGet(metastore::getAllDatabases);
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schema : schemas) {
            Optional<List<String>> allTables = metastore.getAllTables(schema);
            List<SchemaTableName> schemaTableNames = allTables
                    .map(tables -> tables.stream().map(table -> new SchemaTableName(schema, table)).collect(toList()))
                    .orElse(emptyList());
            tableNames.addAll(schemaTableNames);
        }
        return tableNames.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle tbl = (IcebergTableHandle) tableHandle;
        Configuration configuration = getConfiguration(session, tbl.getSchemaName());
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(tbl.getSchemaName(), tbl.getTableName(), configuration);
        List<HiveColumnHandle> columns = icebergUtil.getColumns(icebergTable.schema(), icebergTable.spec(), typeManager);
        return columns.stream().collect(toMap(HiveColumnHandle::getName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        HiveColumnHandle column = (HiveColumnHandle) columnHandle;
        return new ColumnMetadata(column.getName(), typeManager.getType(column.getTypeSignature()));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        SchemaTableName schemaTableName = prefix.toSchemaTableName();
        Configuration configuration = getConfiguration(session, schemaTableName.getSchemaName());
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(schemaTableName.getSchemaName(), schemaTableName.getTableName(), configuration);
        List<ColumnMetadata> columnMetadatas = getColumnMetadatas(icebergTable);
        return ImmutableMap.<SchemaTableName, List<ColumnMetadata>>builder().put(new SchemaTableName(schemaTableName.getSchemaName(), schemaTableName.getTableName()), columnMetadatas).build();
    }

    /**
     * Get statistics for table for given filtering constraint.
     */
    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint)
    {
        return TableStatistics.empty();
    }

    /**
     * Creates a table using the specified table metadata.
     */
    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        Schema schema = new Schema(toIceberg(columns));

        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        partitionedBy.forEach(builder::identity);
        PartitionSpec partitionSpec = builder.build();

        Configuration configuration = getConfiguration(session, schemaName);
        HiveTables hiveTables = icebergUtil.getTable(configuration);

        if (ignoreExisting) {
            Optional<Table> table = metastore.getTable(schemaName, tableName);
            if (table.isPresent()) {
                return;
            }
        }
        hiveTables.create(schema, partitionSpec, schemaName, tableName);
    }

    /**
     * Get the physical layout for a new table.
     */
    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return Optional.empty();
    }

    /**
     * Get the physical layout for a inserting into an existing table.
     */
    @Override
    public Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // TODO We need to provide proper partitioning handle and columns here, we need it for bucketing support but for non bucketed tables it is not required.
        return Optional.empty();
    }

    /**
     * Begin the atomic creation of a table with data.
     */
    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorNewTableLayout> layout)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Schema schema = new Schema(toIceberg(tableMetadata.getColumns()));
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        partitionedBy.forEach(builder::identity);
        PartitionSpec partitionSpec = builder.build();

        Configuration configuration = getConfiguration(session, schemaName);
        HiveTables table = icebergUtil.getTable(configuration);
        //TODO see if there is a way to store this as transaction state.
        this.transaction = table.beginCreate(schema, partitionSpec, schemaName, tableName);
        List<HiveColumnHandle> hiveColumnHandles = icebergUtil.getColumns(schema, partitionSpec, typeManager);
        LocationHandle locationHandle = locationService.forNewTable(metastore, session, schemaName, tableName);
        Path targetPath = locationService.getQueryWriteInfo(locationHandle).getTargetPath();
        return new IcebergInsertTableHandle(
                schemaName,
                tableName,
                SchemaParser.toJson(transaction.table().schema()),
                PartitionSpecParser.toJson(partitionSpec),
                hiveColumnHandles,
                targetPath.toString(),
                FileFormat.PARQUET);
    }

    /**
     * Finish a table creation with data after the data is written.
     */
    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return finishInsert(session, (IcebergInsertTableHandle) tableHandle, fragments, computedStatistics);
    }

    /**
     * Begin insert query
     */
    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle tbl = (IcebergTableHandle) tableHandle;
        Configuration configuration = getConfiguration(session, tbl.getSchemaName());
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(tbl.getSchemaName(), tbl.getTableName(), configuration);
        this.transaction = icebergTable.newTransaction();
        String location = icebergTable.location();
        List<HiveColumnHandle> columns = icebergUtil.getColumns(icebergTable.schema(), icebergTable.spec(), typeManager);
        return new IcebergInsertTableHandle(
                tbl.getSchemaName(),
                tbl.getTableName(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                columns,
                icebergUtil.getDataPath(location),
                icebergUtil.getFileFormat(icebergTable));
    }

    /**
     * Finish insert query
     */
    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        List<CommitTaskData> commitTasks = fragments.stream().map(slice -> jsonCodec.fromJson(slice.getBytes())).collect(toList());
        IcebergInsertTableHandle icebergTable = (IcebergInsertTableHandle) insertHandle;
        org.apache.iceberg.types.Type[] partitionColumnTypes = icebergTable.getInputColumns().stream()
                .filter(HiveColumnHandle::isPartitionKey)
                .map(col -> typeManager.getType(col.getTypeSignature()))
                .map(TypeConveter::convert)
                .toArray(org.apache.iceberg.types.Type[]::new);
        AppendFiles appendFiles = transaction.newFastAppend();
        for (CommitTaskData commitTaskData : commitTasks) {
            DataFiles.Builder builder;
            builder = DataFiles.builder(transaction.table().spec())
                    .withInputFile(HadoopInputFile.fromLocation(commitTaskData.getPath(), getConfiguration(session, icebergTable.getSchemaName())))
                    .withFormat(icebergTable.getFileFormat())
                    .withMetrics(MetricsParser.fromJson(commitTaskData.getMetricsJson()));

            if (!transaction.table().spec().fields().isEmpty()) {
                builder.withPartition(PartitionData.fromJson(commitTaskData.getPartitionDataJson(), partitionColumnTypes));
            }
            appendFiles.appendFile(builder.build());
        }

        appendFiles.commit();
        transaction.commitTransaction();
        return Optional.of(new HiveWrittenPartitions(commitTasks.stream().map(CommitTaskData::getPartitionPath).collect(toList())));
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableLayoutHandle layoutHandle)
    {
        // TODO this is passed to event stream so we may get wrong metrics if this does not have correct info
        return Optional.empty();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;

        if (!metastore.getTable(handle.getSchemaName(), handle.getTableName()).isPresent()) {
            throw new TableNotFoundException(schemaTableName(tableHandle));
        }
        metastore.dropTable(session, handle.getSchemaName(), handle.getTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        metastore.renameTable(handle.getSchemaName(), handle.getTableName(), newTableName.getSchemaName(), newTableName.getTableName());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Configuration configuration = getConfiguration(session, handle.getSchemaName());
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(handle.getSchemaName(), handle.getTableName(), configuration);
        icebergTable.updateSchema().addColumn(column.getName(), TypeConveter.convert(column.getType())).commit();
    }

    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        HiveColumnHandle handle = (HiveColumnHandle) column;
        Configuration configuration = getConfiguration(session, icebergTableHandle.getSchemaName());
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(icebergTableHandle.getSchemaName(), icebergTableHandle.getTableName(), configuration);
        icebergTable.updateSchema().deleteColumn(handle.getName()).commit();
    }

    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        HiveColumnHandle columnHandle = (HiveColumnHandle) source;
        Configuration configuration = getConfiguration(session, icebergTableHandle.getSchemaName());
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(icebergTableHandle.getSchemaName(), icebergTableHandle.getTableName(), configuration);
        icebergTable.updateSchema().renameColumn(columnHandle.getName(), target).commit();
    }

    private ConnectorTableMetadata getTableMetadata(String schema, String tableName, ConnectorSession session)
    {
        Optional<Table> table = metastore.getTable(schema, tableName);
        if (!table.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(schema, tableName));
        }
        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session, schema), new Path("file:///tmp"));

        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(schema, tableName, configuration);

        List<ColumnMetadata> columns = getColumnMetadatas(icebergTable);

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(TABLE_PROPERTIES, icebergTable.properties());
        properties.put(SCHEMA_PROPERTY, icebergTable.schema());
        properties.put(PARTITION_SPEC_PROPERTY, icebergTable.spec());

        return new ConnectorTableMetadata(new SchemaTableName(schema, tableName), columns, properties.build(), Optional.empty());
    }

    private List<ColumnMetadata> getColumnMetadatas(org.apache.iceberg.Table icebergTable)
    {
        return icebergTable.schema().columns().stream()
                .map(c -> new ColumnMetadata(c.name(), TypeConveter.convert(c.type(), typeManager)))
                .collect(toList());
    }

    private List<Types.NestedField> toIceberg(List<ColumnMetadata> columns)
    {
        List<Types.NestedField> icebergColumns = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            if (!column.isHidden()) {
                String name = column.getName();
                Type type = column.getType();
                org.apache.iceberg.types.Type icebergType = TypeConveter.convert(type);
                icebergColumns.add(optional(icebergColumns.size(), name, icebergType));
            }
        }
        return icebergColumns;
    }

    private Configuration getConfiguration(ConnectorSession session, String schemaName)
    {
        return hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session, schemaName), new Path("file:///tmp"));
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return updateRowIdHandle();
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        return true;
    }

    @Override
    public OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        IcebergTableLayoutHandle layoutHandle = (IcebergTableLayoutHandle) tableLayoutHandle;

        final Configuration configuration = getConfiguration(session, handle.getSchemaName());
        org.apache.iceberg.Table icebergTable = icebergUtil.getIcebergTable(handle.getSchemaName(), handle.getTableName(), configuration);
        icebergTable.newDelete()
                .deleteFromRowFilter(ExpressionConverter.toIceberg(layoutHandle.getPredicates(), session))
                .commit();

        // TODO in case of iceberg it should be possible to return number of deleted records easily.
        return OptionalLong.empty();
    }

    @Override
    public void rollback()
    {
        metastore.rollback();
    }

    @Override
    public void commit()
    {
        metastore.commit();
    }

    SemiTransactionalHiveMetastore getMetastore()
    {
        return metastore;
    }
}
