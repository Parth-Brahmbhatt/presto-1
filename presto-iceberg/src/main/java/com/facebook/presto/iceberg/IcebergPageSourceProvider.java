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

import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePageSource;
import com.facebook.presto.hive.HivePageSourceProvider.ColumnMapping;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.hive.parquet.ParquetPageSource;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.predicate.Predicate;
import com.facebook.presto.parquet.reader.MetadataReader;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.getParquetTupleDomain;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.getParquetType;
import static com.facebook.presto.iceberg.IcebergUtil.SNAPSHOT_ID;
import static com.facebook.presto.iceberg.IcebergUtil.SNAPSHOT_TIMESTAMP_MS;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.getDescriptors;
import static com.facebook.presto.parquet.predicate.PredicateUtils.buildPredicate;
import static com.facebook.presto.parquet.predicate.PredicateUtils.predicateMatches;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class IcebergPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final HiveClientConfig hiveClientConfig;
    private FileFormatDataSourceStats fileFormatDataSourceStats;

    @Inject
    public IcebergPageSourceProvider(
            HiveClientConfig hiveClientConfig,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            FileFormatDataSourceStats fileFormatDataSourceStats)
    {
        requireNonNull(hiveClientConfig, "hiveClientConfig is null");
        this.hiveClientConfig = hiveClientConfig;
        this.hdfsEnvironment = hdfsEnvironment;
        this.typeManager = typeManager;
        this.fileFormatDataSourceStats = fileFormatDataSourceStats;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns)
    {
        final IcebergSplit icebergSplit = (IcebergSplit) split;
        final Path path = new Path(icebergSplit.getPath());
        final long start = icebergSplit.getStart();
        final long length = icebergSplit.getLength();
        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());
        return createParquetPageSource(hdfsEnvironment,
                session.getUser(),
                hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session, icebergSplit.getDatabase(), icebergSplit.getTable()), path),
                path,
                start,
                length,
                hiveColumns,
                icebergSplit.getNameToId(),
                hiveClientConfig.isUseParquetColumnNames(),
                typeManager,
                icebergSplit.getEffectivePredicate(),
                icebergSplit.getPartitionKeys(),
                fileFormatDataSourceStats,
                icebergSplit.getSnapshotId(),
                icebergSplit.getSnapshotTimestamp());
    }

    public ConnectorPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            List<HiveColumnHandle> columns,
            Map<String, Integer> icebergNameToId,
            boolean useParquetColumnNames,
            TypeManager typeManager,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HivePartitionKey> partitionKeys,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            Long snapshotId,
            Long snapshotTimeStamp)
    {
        AggregatedMemoryContext systemMemoryContext = AggregatedMemoryContext.newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            final long fileSize = fileSystem.getFileStatus(path).getLen();
            dataSource = buildHdfsParquetDataSource(fileSystem, path, start, length, fileSize, fileFormatDataSourceStats);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(fileSystem, path, fileSize);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            // We need to transform columns so they have the parquet column name and not table column name.
            // In order to make that transformation we need to pass the iceberg schema (not the hive schema) in split and
            // use that here to map from iceberg schema column name to ID, lookup parquet column with same ID and all of its children
            // and use the index of all those columns as requested schema.

            final List<HiveColumnHandle> parquetColumns = convertToParquetNames(columns, icebergNameToId, fileSchema);
            List<org.apache.parquet.schema.Type> fields = parquetColumns.stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .map(column -> getParquetType(column, fileSchema, true)) // we always use parquet column names in case of iceberg.
                    .filter(Objects::nonNull)
                    .collect(toList());

            MessageType requestedSchema = new MessageType(fileSchema.getName(), fields);

            List<BlockMetaData> blocks = new ArrayList<>();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= start && firstDataPage < start + length) {
                    blocks.add(block);
                }
            }

            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);
            final ParquetDataSource finalDataSource = dataSource;
            blocks = blocks.stream()
                    .filter(block -> predicateMatches(parquetPredicate, block, finalDataSource, descriptorsByPath, parquetTupleDomain))
                    .collect(toList());
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    messageColumnIO,
                    blocks,
                    dataSource,
                    systemMemoryContext);

            final ImmutableList.Builder<ColumnMapping> mappingBuilder = new ImmutableList.Builder<>();
            mappingBuilder.addAll(buildColumnMappings(partitionKeys,
                    columns,
                    Collections.EMPTY_LIST,
                    Collections.emptyMap(),
                    path,
                    OptionalInt.empty()));

            setIfPresent(mappingBuilder, getColumnHandle(SNAPSHOT_ID, columns), String.valueOf(snapshotId));
            setIfPresent(mappingBuilder, getColumnHandle(SNAPSHOT_TIMESTAMP_MS, columns), String.valueOf(snapshotTimeStamp));

            return new HivePageSource(
                    mappingBuilder.build(),
                    Optional.empty(),
                    DateTimeZone.UTC,
                    typeManager,
                    new ParquetPageSource(
                            parquetReader,
                            fileSchema,
                            messageColumnIO,
                            typeManager,
                            new Properties(),
                            columns.stream().filter(c -> c.getColumnType() == REGULAR).collect(toList()),
                            effectivePredicate,
                            useParquetColumnNames));
        }
        catch (Exception e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    /**
     * This method maps the iceberg column names to corresponding parquet column names by matching their Ids rather then relying on name or index.
     * If the parquet schema has at least one field with no Id then the method assumes this is a case of a migrated hive table and it will return the original column name as is.
     * If all the parquet schema fields have the Id but the iceberg schema ids do not match any column the method throws an exception indicating data issue.
     * @param columns iceberg columns
     * @param icebergNameToId
     * @param parquetSchema
     * @return columns with iceberg column names replaced with parquet column names.
     */
    private List<HiveColumnHandle> convertToParquetNames(List<HiveColumnHandle> columns, Map<String, Integer> icebergNameToId, MessageType parquetSchema)
    {
        // TODO do it at top level rather than repeating this work per split.
        final List<org.apache.parquet.schema.Type> fields = parquetSchema.getFields();
        final List<HiveColumnHandle> result = new ArrayList<>();
        Map<Integer, String> icebergIdToName = icebergNameToId.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        Map<String, HiveColumnHandle> columnNameToColumnHandle = columns.stream()
                .collect(Collectors.toMap(HiveColumnHandle::getName, Function.identity()));

        for (int index = 0; index < fields.size(); index++) {
            final Type field = fields.get(index);
            if (field.getId() != null) {
                final String parquetName = field.getName();
                final String icebergColumnName = icebergIdToName.get(field.getId().intValue());
                if (icebergColumnName == null) {
                    // the column is present in parquet but dropped from iceberg schema, so ignoring the column.
                    continue;
                }
                else {
                    final HiveColumnHandle column = columnNameToColumnHandle.get(icebergColumnName);
                    result.add(new HiveColumnHandle(parquetName, column.getHiveType(), column.getTypeSignature(), column.getHiveColumnIndex(), column.getColumnType(), column.getComment()));
                }
            }
            else {
                // the column id is null, so this is a case of migrated table, we will make the assumption that no
                // columns were dropped so we will just return the column at same index.
                result.add(columns.get(index));
            }
        }
        return result;
    }

    private final Optional<HiveColumnHandle> getColumnHandle(String columnName, List<HiveColumnHandle> columns)
    {
        // Assumes the call is only made for columns that are always present
        return columns.stream().filter(columnHandle -> columnHandle.getName().equals(columnName)).findFirst();
    }

    private void setIfPresent(ImmutableList.Builder<ColumnMapping> mappingBuilder, Optional<HiveColumnHandle> columnHandle, String value)
    {
        if (columnHandle.isPresent()) {
            mappingBuilder.add(ColumnMapping.prefilled(columnHandle.get(), value, Optional.empty()));
        }
    }
}
