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
import io.prestosql.iceberg.type.TypeConveter;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.prestosql.iceberg.IcebergUtil.getIdentityPartitions;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static java.util.function.Function.identity;

public class PartitionTable
        implements SystemTable
{
    private final IcebergTableHandle tableHandle;
    private final ConnectorSession session;
    private final IcebergUtil icebergUtil;
    private final Configuration configuration;
    private final TypeManager typeManager;
    private Table icebergTable;
    private Map<Integer, Type.PrimitiveType> idToTypeMapping;
    private List<Types.NestedField> nonPartitionPrimitiveColumns;
    private List<io.prestosql.spi.type.Type> partitionColumnTypes;
    private List<io.prestosql.spi.type.Type> resultTypes;
    private List<io.prestosql.spi.type.RowType> columnMetricTypes;

    // TODO heck as system table calls do not have connector classloader
    private final ClassLoader connectorClassLoader = Thread.currentThread().getContextClassLoader();

    public PartitionTable(IcebergTableHandle tableHandle, ConnectorSession session, IcebergUtil icebergUtil, Configuration configuration, TypeManager typeManager)
    {
        this.tableHandle = tableHandle;
        this.session = session;
        this.icebergUtil = icebergUtil;
        this.configuration = configuration;
        this.typeManager = typeManager;
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        this.icebergTable = icebergUtil.getIcebergTable(tableHandle.getSchemaName(), tableHandle.getTableName(), configuration);
        this.idToTypeMapping = icebergTable.schema().columns().stream()
                .filter(column -> column.type().isPrimitiveType())
                .collect(Collectors.toMap(Types.NestedField::fieldId, (column) -> column.type().asPrimitiveType()));

        List<Types.NestedField> columns = icebergTable.schema().columns();
        List<PartitionField> partitionFields = icebergTable.spec().fields();

        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();

        List<ColumnMetadata> partitionColumnsMetadata = getPartitionColumnsMetadata(partitionFields, icebergTable.schema());
        this.partitionColumnTypes = partitionColumnsMetadata.stream().map(m -> m.getType()).collect(Collectors.toList());
        columnMetadataBuilder.addAll(partitionColumnsMetadata);

        Set<Integer> identityPartitionIds = getIdentityPartitions(icebergTable.spec()).keySet().stream()
                .map(partitionField -> partitionField.sourceId())
                .collect(Collectors.toSet());

        this.nonPartitionPrimitiveColumns = columns.stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) && column.type().isPrimitiveType())
                .collect(Collectors.toList());

        List<String> perPartitionMetrics = ImmutableList.of("record_count", "file_count", "total_size");
        perPartitionMetrics.stream().forEach(metric -> columnMetadataBuilder.add(new ColumnMetadata(metric, BIGINT)));

        List<ColumnMetadata> columnMetricsMetadata = getColumnMetadata(nonPartitionPrimitiveColumns);
        columnMetadataBuilder.addAll(columnMetricsMetadata);

        this.columnMetricTypes = columnMetricsMetadata.stream().map(m -> (RowType) m.getType()).collect(Collectors.toList());

        ImmutableList<ColumnMetadata> columnMetadata = columnMetadataBuilder.build();
        this.resultTypes = columnMetadata.stream().map(m -> m.getType()).collect(Collectors.toList());
        return new ConnectorTableMetadata(new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()), columnMetadata);
    }

    private final List<ColumnMetadata> getPartitionColumnsMetadata(List<PartitionField> fields, Schema schema)
    {
        return fields.stream()
                .map(field -> new ColumnMetadata(field.name(), TypeConveter.convert(field.transform().getResultType(schema.findType(field.sourceId())), typeManager)))
                .collect(Collectors.toList());
    }

    private final List<ColumnMetadata> getColumnMetadata(List<Types.NestedField> columns)
    {
        return columns.stream().map(column -> new ColumnMetadata(column.name(),
                RowType.from(
                        ImmutableList.of(
                                new RowType.Field(Optional.of("min"), TypeConveter.convert(column.type(), typeManager)),
                                new RowType.Field(Optional.of("max"), TypeConveter.convert(column.type(), typeManager)),
                                new RowType.Field(Optional.of("nullCount"), BIGINT)))))
                .collect(Collectors.toList());
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        // TODO instead of cursor use pageSource method.
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(connectorClassLoader);
            TableScan tableScan = getTableScan(constraint);
            Map<StructLikeWrapper, Partition> partitions = getPartitions(tableScan);
            return buildRecordCursor(partitions, icebergTable.spec().fields());
        }
        finally {
            // TODO heck, set the original classloader
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    private Map<StructLikeWrapper, Partition> getPartitions(TableScan tableScan)
    {
        Map<StructLikeWrapper, Partition> partitions = new HashMap<>();

        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
                DataFile dataFile = fileScanTask.file();
                StructLike partitionStruct = dataFile.partition();
                StructLikeWrapper partitionWrapper = StructLikeWrapper.wrap(partitionStruct);
                if (!partitions.containsKey(partitionWrapper)) {
                    Partition partition = new Partition(partitionStruct,
                            dataFile.recordCount(),
                            dataFile.fileSizeInBytes(),
                            toMap(dataFile.lowerBounds()),
                            toMap(dataFile.upperBounds()),
                            dataFile.nullValueCounts());
                    partitions.put(partitionWrapper, partition);
                    continue;
                }

                Partition partition = partitions.get(partitionWrapper);
                partition.incrementFileCount();
                partition.incrementRecordCount(dataFile.recordCount());
                partition.incrementSize(dataFile.fileSizeInBytes());
                partition.updateMin(toMap(dataFile.lowerBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                partition.updateMax(toMap(dataFile.upperBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                partition.updateNullCount(dataFile.nullValueCounts());
            }
        }
        catch (IOException e) {
            // Add Iceberg error codes for all iceberg errors.
            new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, e);
        }
        return partitions;
    }

    private final RecordCursor buildRecordCursor(Map<StructLikeWrapper, Partition> partitions, List<PartitionField> partitionFields)
    {
        ImmutableList.Builder<List<Object>> records = new ImmutableList.Builder();
        List<Type> partitionTypes = partitionTypes(partitionFields);
        List<? extends Class<?>> partitionColumnClass = partitionTypes.stream().map(type -> type.typeId().javaClass()).collect(Collectors.toList());
        int columnCounts = partitionColumnTypes.size() + 3 + columnMetricTypes.size();
        for (PartitionTable.Partition partition : partitions.values()) {
            List<Object> rows = new ArrayList<>(columnCounts);
            StructLike partitionColumnValues = partition.getValues();
            // add data for partition columns
            for (int i = 0; i < partitionColumnTypes.size(); i++) {
                rows.add(convert(partitionColumnValues.get(i, partitionColumnClass.get(i)), partitionTypes.get(i)));
            }

            // add the top level metrics.
            rows.add(partition.getRecordCount());
            rows.add(partition.getFileCount());
            rows.add(partition.getSize());

            // add column level metrics
            for (int i = 0; i < columnMetricTypes.size(); i++) {
                Integer fieldId = nonPartitionPrimitiveColumns.get(i).fieldId();
                Type.PrimitiveType type = idToTypeMapping.get(fieldId);
                Object min = convert(partition.getMinValues().get(fieldId), type);
                Object max = convert(partition.getMaxValues().get(fieldId), type);
                Long nullCount = partition.getNullCounts().get(fieldId);
                rows.add(getColumnMetricBlock(columnMetricTypes.get(i), min, max, nullCount));
            }

            records.add(rows);
        }

        return new InMemoryRecordSet(resultTypes, records.build()).cursor();
    }

    private List<Type> partitionTypes(List<PartitionField> partitionFields)
    {
        ImmutableList.Builder<Type> partitionTypeBuilder = ImmutableList.builder();
        for (int i = 0; i < partitionFields.size(); i++) {
            PartitionField partitionField = partitionFields.get(i);
            Type.PrimitiveType sourceType = idToTypeMapping.get(partitionField.sourceId());
            Type type = partitionField.transform().getResultType(sourceType);
            partitionTypeBuilder.add(type);
        }
        return partitionTypeBuilder.build();
    }

    private Block getColumnMetricBlock(RowType columnMetricType, Object min, Object max, Long nullCount)
    {
        BlockBuilder rowBlockBuilder = columnMetricType.createBlockBuilder(null, 1);
        BlockBuilder builder = rowBlockBuilder.beginBlockEntry();
        List<RowType.Field> fields = columnMetricType.getFields();
        TypeUtils.writeNativeValue(fields.get(0).getType(), builder, min);
        TypeUtils.writeNativeValue(fields.get(1).getType(), builder, max);
        TypeUtils.writeNativeValue(fields.get(2).getType(), builder, nullCount);

        rowBlockBuilder.closeEntry();
        return columnMetricType.getObject(rowBlockBuilder, 0);
    }

    private final TableScan getTableScan(TupleDomain<Integer> constraint)
    {
        List<HiveColumnHandle> partitionColumns = icebergUtil.getPartitionColumns(icebergTable.schema(), icebergTable.spec(), typeManager);
        Map<Integer, HiveColumnHandle> fieldIdToColumnHandle = IntStream.range(0, partitionColumnTypes.size())
                .boxed()
                .collect(Collectors.toMap(identity(), partitionColumns::get));
        TupleDomain<HiveColumnHandle> predicates = constraint.transform(fieldIdToColumnHandle::get);

        return icebergUtil.getTableScan(session, predicates, tableHandle.getAtId(), icebergTable);
    }

    private Map<Integer, Object> toMap(Map<Integer, ByteBuffer> idToMetricMap)
    {
        Map<Integer, Object> map = new HashMap<>();
        for (Map.Entry<Integer, ByteBuffer> idToMetricEntry : idToMetricMap.entrySet()) {
            Integer id = idToMetricEntry.getKey();
            Type.PrimitiveType type = idToTypeMapping.get(id);
            map.put(id, Conversions.fromByteBuffer(type, idToMetricEntry.getValue()));
        }
        return map;
    }

    private static Object convert(Object value, Type type)
    {
        if (value == null) {
            return null;
        }
        else if (type instanceof Types.StringType) {
            return value.toString();
        }
        else if (type instanceof Types.BinaryType) {
            // TODO the client sees the bytearray's tostring ouput instead of seeing actual bytes, needs to be fixed.
            ByteBuffer buffer = (ByteBuffer) value;
            return buffer.array();
        }
        else if (type instanceof Types.TimestampType) {
            long utcMillis = TimeUnit.MICROSECONDS.toMillis((Long) value);
            Types.TimestampType timestampType = (Types.TimestampType) type;
            if (timestampType.shouldAdjustToUTC()) {
                return packDateTimeWithZone(utcMillis, TimeZoneKey.UTC_KEY);
            }
            else {
                return utcMillis;
            }
        }
        else if (type instanceof Types.FloatType) {
            return Float.floatToIntBits((Float) value);
        }
        return value;
    }

    private class Partition
    {
        private StructLike values;
        private long recordCount;
        private long fileCount;
        private long size;
        private Map<Integer, Object> minValues;
        private Map<Integer, Object> maxValues;
        private Map<Integer, Long> nullCounts;
        private Set<Integer> corruptedStats;

        public Partition(StructLike values, long recordCount, long size, Map<Integer, Object> minValues, Map<Integer, Object> maxValues, Map<Integer, Long> nullCounts)
        {
            this.values = values;
            this.recordCount = recordCount;
            this.fileCount = 1;
            this.size = size;
            this.minValues = minValues;
            this.maxValues = maxValues;
            // we are assuming if minValues is not present, max will be not be present either.
            this.corruptedStats = nonPartitionPrimitiveColumns.stream()
                    .map(field -> field.fieldId())
                    .filter(id -> !minValues.containsKey(id) && (!nullCounts.containsKey(id) || nullCounts.get(id) != recordCount)).collect(Collectors.toSet());
            this.nullCounts = nullCounts.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)); // original nullcounts is immutable
        }

        public StructLike getValues()
        {
            return values;
        }

        public long getRecordCount()
        {
            return recordCount;
        }

        public long getFileCount()
        {
            return fileCount;
        }

        public long getSize()
        {
            return size;
        }

        public Map<Integer, Object> getMinValues()
        {
            return minValues;
        }

        public Map<Integer, Object> getMaxValues()
        {
            return maxValues;
        }

        public Map<Integer, Long> getNullCounts()
        {
            return nullCounts;
        }

        public void incrementRecordCount(long count)
        {
            this.recordCount = this.recordCount + count;
        }

        public void incrementFileCount()
        {
            this.fileCount = this.fileCount + 1;
        }

        public void incrementSize(long numberOfBytes)
        {
            this.size = this.size + numberOfBytes;
        }

        /**
         * The update logic is built with the following rules:
         * bounds is null => if any file has a missing bound for a column, that bound will not be reported
         * bounds is missing id => not reported in Parquet => that bound will not be reported
         * bound value is null => not an expected case
         * bound value is present => this is the normal case and bounds will be reported correctly
         */

        public void updateMin(Map<Integer, Object> lowerBounds, Map<Integer, Long> nullCounts, Long recordCount)
        {
            updateStats(this.minValues, lowerBounds, nullCounts, recordCount, i -> (i > 0));
        }

        public void updateMax(Map<Integer, Object> upperBounds, Map<Integer, Long> nullCounts, Long recordCount)
        {
            updateStats(this.maxValues, upperBounds, nullCounts, recordCount, i -> (i < 0));
        }

        private void updateStats(Map<Integer, Object> current, Map<Integer, Object> newStat, Map<Integer, Long> nullCounts, Long recordCount, Predicate<Integer> predicate)
        {
            for (Types.NestedField column : nonPartitionPrimitiveColumns) {
                int id = column.fieldId();

                if (corruptedStats.contains(id)) {
                    continue;
                }

                Object newValue = newStat.get(id);
                // it is expected to not have min/max if all values are null for a column in the datafile and it is not a case of corrupted stats.
                if (newValue == null) {
                    if (nullCounts.get(id) == null || nullCounts.get(id) != recordCount) {
                        current.remove(id);
                        corruptedStats.add(id);
                    }
                    continue;
                }

                Object oldValue = current.get(id);
                if (oldValue == null) {
                    current.put(id, newValue);
                }
                else {
                    Comparator<Object> comparator = Comparators.forType(idToTypeMapping.get(id));
                    if (predicate.test(comparator.compare(oldValue, newValue))) {
                        current.put(id, newValue);
                    }
                }
            }
        }

        public void updateNullCount(Map<Integer, Long> nullCounts)
        {
            for (Map.Entry<Integer, Long> entry : nullCounts.entrySet()) {
                Long nullCount = this.nullCounts.getOrDefault(entry.getKey(), 0L);
                this.nullCounts.put(entry.getKey(), entry.getValue() + nullCount);
            }
        }
    }
}
