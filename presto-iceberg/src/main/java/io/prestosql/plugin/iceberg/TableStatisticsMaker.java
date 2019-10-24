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
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.DoubleRange;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.TypeManager;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.prestosql.plugin.iceberg.IcebergUtil.getColumns;
import static io.prestosql.plugin.iceberg.IcebergUtil.getIdentityPartitions;
import static io.prestosql.plugin.iceberg.Partition.toMap;
import static io.prestosql.plugin.iceberg.TypeConverter.toPrestoType;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class TableStatisticsMaker
{
    private final TypeManager typeManager;
    private final Table icebergTable;

    private TableStatisticsMaker(TypeManager typeManager, Table icebergTable)
    {
        this.typeManager = typeManager;
        this.icebergTable = icebergTable;
    }

    public static TableStatistics getTableStatistics(TypeManager typeManager, Constraint constraint, IcebergTableHandle tableHandle, Table icebergTable)
    {
        return new TableStatisticsMaker(typeManager, icebergTable).makeTableStatistics(tableHandle, constraint);
    }

    private TableStatistics makeTableStatistics(IcebergTableHandle tableHandle, Constraint constraint)
    {
        if (tableHandle.getSnapshotId().isEmpty() || constraint.getSummary().isNone()) {
            return TableStatistics.empty();
        }

        TupleDomain<IcebergColumnHandle> intersection = constraint.getSummary()
                .transform(IcebergColumnHandle.class::cast)
                .intersect(tableHandle.getEnforcedPredicate());

        if (intersection.isNone()) {
            return TableStatistics.empty();
        }

        List<Types.NestedField> columns = icebergTable.schema().columns();

        Map<Integer, Type.PrimitiveType> idToTypeMapping = columns.stream()
                .filter(column -> column.type().isPrimitiveType())
                .collect(Collectors.toMap(Types.NestedField::fieldId, column -> column.type().asPrimitiveType()));
        List<PartitionField> partitionFields = icebergTable.spec().fields();

        Set<Integer> identityPartitionIds = getIdentityPartitions(icebergTable.spec()).keySet().stream()
                .map(PartitionField::sourceId)
                .collect(toSet());

        List<Types.NestedField> nonPartitionPrimitiveColumns = columns.stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) && column.type().isPrimitiveType())
                .collect(toImmutableList());

        List<Type> icebergPartitionTypes = partitionTypes(partitionFields, idToTypeMapping);
        List<IcebergColumnHandle> columnHandles = getColumns(icebergTable.schema(), icebergTable.spec(), typeManager);
        Map<Integer, IcebergColumnHandle> idToColumnHandle = columnHandles.stream()
                .collect(toUnmodifiableMap(IcebergColumnHandle::getId, identity()));

        ImmutableMap.Builder<Integer, ColumnFieldDetails> idToDetailsBuilder = ImmutableMap.builder();
        for (int index = 0; index < partitionFields.size(); index++) {
            PartitionField field = partitionFields.get(index);
            Type type = icebergPartitionTypes.get(index);
            idToDetailsBuilder.put(field.sourceId(), new ColumnFieldDetails(
                    field,
                    idToColumnHandle.get(field.sourceId()),
                    type,
                    toPrestoType(type, typeManager),
                    type.typeId().javaClass()));
        }
        Map<Integer, ColumnFieldDetails> idToDetails = idToDetailsBuilder.build();

        TableScan tableScan = icebergTable.newScan()
                .filter(toIcebergExpression(intersection))
                .useSnapshot(tableHandle.getSnapshotId().get())
                .includeColumnStats();

        Partition summary = null;
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
                DataFile dataFile = fileScanTask.file();
                if (!dataFileMatches(
                        dataFile,
                        constraint,
                        idToTypeMapping,
                        partitionFields,
                        idToDetails)) {
                    continue;
                }

                if (summary == null) {
                    summary = new Partition(
                            idToTypeMapping,
                            nonPartitionPrimitiveColumns,
                            dataFile.partition(),
                            dataFile.recordCount(),
                            dataFile.fileSizeInBytes(),
                            toMap(idToTypeMapping, dataFile.lowerBounds()),
                            toMap(idToTypeMapping, dataFile.upperBounds()),
                            dataFile.nullValueCounts(),
                            dataFile.columnSizes());
                }
                else {
                    summary.incrementFileCount();
                    summary.incrementRecordCount(dataFile.recordCount());
                    summary.incrementSize(dataFile.fileSizeInBytes());
                    updateSummaryMin(summary, partitionFields, toMap(idToTypeMapping, dataFile.lowerBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                    updateSummaryMax(summary, partitionFields, toMap(idToTypeMapping, dataFile.upperBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                    summary.updateNullCount(dataFile.nullValueCounts());
                    updateColumnSizes(summary, dataFile.columnSizes());
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (summary == null) {
            return TableStatistics.empty();
        }

        ImmutableMap.Builder<ColumnHandle, ColumnStatistics> columnHandleBuilder = ImmutableMap.builder();
        double recordCount = summary.getRecordCount();
        for (IcebergColumnHandle columnHandle : idToColumnHandle.values()) {
            int fieldId = columnHandle.getId();
            ColumnStatistics.Builder columnBuilder = new ColumnStatistics.Builder();
            Long nullCount = summary.getNullCounts().get(fieldId);
            if (nullCount != null) {
                columnBuilder.setNullsFraction(Estimate.of(nullCount / recordCount));
            }
            if (summary.getColumnSizes() != null) {
                Long columnSize = summary.getColumnSizes().get(fieldId);
                if (columnSize != null) {
                    columnBuilder.setDataSize(Estimate.of(columnSize));
                }
            }
            Object min = summary.getMinValues().get(fieldId);
            Object max = summary.getMaxValues().get(fieldId);
            if (min instanceof Number && max instanceof Number) {
                columnBuilder.setRange(Optional.of(new DoubleRange(((Number) min).doubleValue(), ((Number) max).doubleValue())));
            }
            columnHandleBuilder.put(columnHandle, columnBuilder.build());
        }
        return new TableStatistics(Estimate.of(recordCount), columnHandleBuilder.build());
    }

    private boolean dataFileMatches(
            DataFile dataFile,
            Constraint constraint,
            Map<Integer, Type.PrimitiveType> idToTypeMapping,
            List<PartitionField> partitionFields,
            Map<Integer, ColumnFieldDetails> fieldDetails)
    {
        TupleDomain<ColumnHandle> constraintSummary = constraint.getSummary();

        Map<ColumnHandle, Domain> domains = constraintSummary.getDomains().get();

        Predicate<Map<ColumnHandle, NullableValue>> predicate = constraint.predicate().orElse(value -> true);

        ImmutableMap.Builder<ColumnHandle, NullableValue> nullableValueBuilder = ImmutableMap.builder();

        for (int index = 0; index < partitionFields.size(); index++) {
            PartitionField field = partitionFields.get(index);
            int fieldId = field.sourceId();
            ColumnFieldDetails details = fieldDetails.get(fieldId);
            IcebergColumnHandle column = details.getColumnHandle();
            Object value = PartitionTable.convert(dataFile.partition().get(index, details.getJavaClass()), idToTypeMapping.get(fieldId));
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value)) {
                return false;
            }
            nullableValueBuilder.put(column, makeNullableValue(details.getPrestoType(), value));
        }

        if (constraint.getPredicateColumns().isPresent()) {
            return predicate.test(nullableValueBuilder.build());
        }

        return true;
    }

    private NullableValue makeNullableValue(io.prestosql.spi.type.Type type, Object value)
    {
        return value == null ? NullableValue.asNull(type) : NullableValue.of(type, value);
    }

    public List<Type> partitionTypes(List<PartitionField> partitionFields, Map<Integer, Type.PrimitiveType> idToTypeMapping)
    {
        ImmutableList.Builder<Type> partitionTypeBuilder = ImmutableList.builder();
        for (PartitionField partitionField : partitionFields) {
            Type.PrimitiveType sourceType = idToTypeMapping.get(partitionField.sourceId());
            Type type = partitionField.transform().getResultType(sourceType);
            partitionTypeBuilder.add(type);
        }
        return partitionTypeBuilder.build();
    }

    private static class ColumnFieldDetails
    {
        private final PartitionField field;
        private final IcebergColumnHandle columnHandle;
        private final Type icebergType;
        private final io.prestosql.spi.type.Type prestoType;
        private final Class<?> javaClass;

        public ColumnFieldDetails(PartitionField field, IcebergColumnHandle columnHandle, Type icebergType, io.prestosql.spi.type.Type prestoType, Class<?> javaClass)
        {
            this.field = requireNonNull(field, "field is null");
            this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
            this.icebergType = requireNonNull(icebergType, "icebergType is null");
            this.prestoType = requireNonNull(prestoType, "prestoType is null");
            this.javaClass = requireNonNull(javaClass, "javaClass is null");
        }

        public PartitionField getField()
        {
            return field;
        }

        public IcebergColumnHandle getColumnHandle()
        {
            return columnHandle;
        }

        public Type getIcebergType()
        {
            return icebergType;
        }

        public io.prestosql.spi.type.Type getPrestoType()
        {
            return prestoType;
        }

        public Class<?> getJavaClass()
        {
            return javaClass;
        }
    }

    public void updateColumnSizes(Partition summary, Map<Integer, Long> addedColumnSizes)
    {
        Map<Integer, Long> columnSizes = summary.getColumnSizes();
        if (!summary.hasValidColumnMetrics() || columnSizes == null || addedColumnSizes == null) {
            return;
        }
        for (Types.NestedField column : summary.getNonPartitionPrimitiveColumns()) {
            int id = column.fieldId();

            Long addedSize = addedColumnSizes.get(id);
            if (addedSize != null) {
                columnSizes.put(id, addedSize + columnSizes.getOrDefault(id, 0L));
            }
        }
    }

    private void updateSummaryMin(Partition summary, List<PartitionField> partitionFields, Map<Integer, Object> lowerBounds, Map<Integer, Long> nullCounts, long recordCount)
    {
        summary.updateStats(summary.getMinValues(), lowerBounds, nullCounts, recordCount, i -> (i > 0));
        updatePartitionedStats(summary, partitionFields, summary.getMinValues(), lowerBounds, i -> (i > 0));
    }

    private void updateSummaryMax(Partition summary, List<PartitionField> partitionFields, Map<Integer, Object> upperBounds, Map<Integer, Long> nullCounts, long recordCount)
    {
        summary.updateStats(summary.getMaxValues(), upperBounds, nullCounts, recordCount, i -> (i < 0));
        updatePartitionedStats(summary, partitionFields, summary.getMaxValues(), upperBounds, i -> (i < 0));
    }

    private void updatePartitionedStats(
            Partition summary,
            List<PartitionField> partitionFields,
            Map<Integer, Object> current,
            Map<Integer, Object> newStats,
            Predicate<Integer> predicate)
    {
        for (PartitionField field : partitionFields) {
            int id = field.sourceId();
            if (summary.getCorruptedStats().contains(id)) {
                continue;
            }

            Object newValue = newStats.get(id);
            if (newValue == null) {
                continue;
            }

            Object oldValue = current.putIfAbsent(id, newValue);
            if (oldValue != null) {
                Comparator<Object> comparator = Comparators.forType(summary.getIdToTypeMapping().get(id));
                if (predicate.test(comparator.compare(oldValue, newValue))) {
                    current.put(id, newValue);
                }
            }
        }
    }
}
