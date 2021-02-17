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

import io.airlift.slice.Slices;
import io.prestosql.plugin.iceberg.util.PageListBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.FileIO;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.prestosql.plugin.iceberg.FilesTable.FILE_FORMAT;
import static io.prestosql.plugin.iceberg.FilesTable.FILE_PATH;
import static io.prestosql.plugin.iceberg.FilesTable.FILE_SIZE_IN_BYTES;
import static io.prestosql.plugin.iceberg.FilesTable.KEY_METADATA;
import static io.prestosql.plugin.iceberg.FilesTable.LAST_METADATA_INDEX;
import static io.prestosql.plugin.iceberg.FilesTable.RECORD_COUNT;
import static io.prestosql.plugin.iceberg.FilesTable.SPLIT_OFFSETS;
import static io.prestosql.plugin.iceberg.PartitionTable.convert;
import static io.prestosql.plugin.iceberg.TypeConverter.toIcebergType;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static java.util.stream.Collectors.toList;
import static org.apache.iceberg.types.Conversions.fromByteBuffer;

public class FilesPageSource
        implements ConnectorPageSource
{
    private FileIO fileIO;
    private List<IcebergColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final String path;
    private long readTimeNanos;
    private Iterator<Page> pages;

    public FilesPageSource(FileIO fileIO, List<IcebergColumnHandle> columnHandles, String path)
    {
        this.fileIO = fileIO;
        this.columnHandles = columnHandles;
        this.columnTypes = columnHandles.stream().map(IcebergColumnHandle::getType).collect(toList());
        this.path = path;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return pages != null && !pages.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (pages != null && pages.hasNext()) {
            return pages.next();
        }

        long start = System.nanoTime();
        PageListBuilder pageListBuilder = new PageListBuilder(columnTypes);

        GenericManifestFile genericManifestFile = new GenericManifestFile(path,
                0, //length is lazily loaded
                -1, //specId
                ManifestContent.DATA, // ManifestContent
                0, //sequenceNumber
                0, //minSequenceNumber
                0L, //snapshotId
                0, //addedFilesCount
                0, //addedRowsCount
                0, //existingFilesCount
                0, //existingRowsCount
                0, //deletedFilesCount
                0, //deletedRowsCount
                null); //partitions
        Iterator<DataFile> dataFileIterator = ManifestFiles.read(genericManifestFile, fileIO).iterator();
        Map valueMap = new HashMap();

        while (dataFileIterator.hasNext()) {
            pageListBuilder.beginRow();

            DataFile dataFile = dataFileIterator.next();
            final StructLike partition = dataFile.partition();
            valueMap.put(FILE_PATH.getId(), dataFile.path());
            valueMap.put(FILE_FORMAT.getId(), dataFile.format().name());
            valueMap.put(RECORD_COUNT.getId(), dataFile.recordCount());
            valueMap.put(FILE_SIZE_IN_BYTES.getId(), dataFile.fileSizeInBytes());
            valueMap.put(KEY_METADATA.getId(), dataFile.keyMetadata() == null ? null : Slices.wrappedBuffer(dataFile.keyMetadata()));
            valueMap.put(SPLIT_OFFSETS.getId(), dataFile.splitOffsets());

            for (IcebergColumnHandle columnHandle : columnHandles) {
                Integer id = columnHandle.getId();
                if (id >= 0) {
//                    final Type type = ((RowType) columnHandle.getType()).getFields().get(4).getType();
//                    org.apache.iceberg.types.Type icebergType = toIcebergType(type);
//                    ColumnStats data = new ColumnStats(
//                            dataFile.columnSizes() != null ? dataFile.columnSizes().get(id) : null,
//                            dataFile.valueCounts() != null ? dataFile.valueCounts().get(id) : null,
//                            dataFile.nullValueCounts() != null ? dataFile.nullValueCounts().get(id) : null,
//                            icebergType.isPrimitiveType() && dataFile.lowerBounds() != null ? convert(fromByteBuffer(icebergType, dataFile.lowerBounds().get(id)), icebergType) : null,
//                            icebergType.isPrimitiveType() && dataFile.upperBounds() != null ? convert(fromByteBuffer(icebergType, dataFile.upperBounds().get(id)), icebergType) : null);
//                    valueMap.put(id, data);

                    final BlockBuilder rowBuilder = pageListBuilder.nextColumn();
                    final BlockBuilder singleRowBlockBuilder = rowBuilder.beginBlockEntry();
                    writeNativeValue(BIGINT, singleRowBlockBuilder, dataFile.columnSizes() != null ? dataFile.columnSizes().get(id) : null);
                    writeNativeValue(BIGINT, singleRowBlockBuilder, dataFile.valueCounts() != null ? dataFile.valueCounts().get(id) : null);
                    writeNativeValue(BIGINT, singleRowBlockBuilder, dataFile.nullValueCounts() != null ? dataFile.nullValueCounts().get(id) : null);

                    // actual primitive Type for bounds
                    final Type type = ((RowType) columnHandle.getType()).getFields().get(4).getType();
                    org.apache.iceberg.types.Type icebergType = toIcebergType(type);
                    if (icebergType.isPrimitiveType() && dataFile.lowerBounds() != null && dataFile.upperBounds() != null) {
                        final Object lowerBound = convert(fromByteBuffer(icebergType, dataFile.lowerBounds().get(id)), icebergType);
                        writeNativeValue(type, singleRowBlockBuilder, lowerBound);
                        final Object upperBound = convert(fromByteBuffer(icebergType, dataFile.upperBounds().get(id)), icebergType);
                        writeNativeValue(type, singleRowBlockBuilder, upperBound);
                    }
                    else {
                        singleRowBlockBuilder.appendNull();
                        singleRowBlockBuilder.appendNull();
                    }
                    rowBuilder.closeEntry();
                }
                else if (id < LAST_METADATA_INDEX) {
//                    int partitionIndex = id - LAST_METADATA_INDEX;
//                    org.apache.iceberg.types.Type icebergType = toIcebergType(columnHandle.getType());
//                    if (partition.size() < partitionIndex) {
//                        valueMap.put(id, null);
//                    } else {
//                        valueMap.put(id, convert(partition.get(partitionIndex, icebergType.typeId().javaClass()), icebergType));
//                    }

                    final org.apache.iceberg.types.Type icebergType = toIcebergType(columnHandle.getType());
                    int partitionIndex = Math.abs(id - LAST_METADATA_INDEX) - 1;
                    if (partition.size() < partitionIndex) {
                        pageListBuilder.appendNull();
                    }
                    else {
                        final BlockBuilder blockBuilder = pageListBuilder.nextColumn();
                        writeNativeValue(
                                columnHandle.getType(),
                                blockBuilder,
                                convert(partition.get(partitionIndex, icebergType.typeId().javaClass()), icebergType));
                    }
                }
                else {
                    final Object value = valueMap.getOrDefault(id, null);
                    if (value instanceof Collection) {
                        writeArray((List) value, ((ArrayType) columnHandle.getType()).getElementType(), pageListBuilder.nextColumn());
                    }
                    else {
                        writeNativeValue(columnHandle.getType(), pageListBuilder.nextColumn(), value);
                    }
                }
            }
//        while (dataFileIterator.hasNext()) {
//            DataFile dataFile = dataFileIterator.next();
//
//            pageListBuilder.beginRow();
//            pageListBuilder.appendVarchar(dataFile.path().toString());
//            pageListBuilder.appendVarchar(dataFile.format().name());
//            pageListBuilder.appendBigint(dataFile.recordCount());
//            pageListBuilder.appendBigint(dataFile.fileSizeInBytes());
//            pageListBuilder.appendNull();
//            pageListBuilder.appendNull();
//            pageListBuilder.appendVarbinary(dataFile.keyMetadata() == null ? null : Slices.wrappedBuffer(dataFile.keyMetadata()));
//            pageListBuilder.appendBigintArray(dataFile.splitOffsets());
//
//            final StructLike partition = dataFile.partition();
//            int partitionIndex = 0;
//
//            for (IcebergColumnHandle columnHandle : columnHandles) {
//                Integer id = columnHandle.getId();
//                if (id > 0) {
//                    final BlockBuilder rowBuilder = pageListBuilder.nextColumn();
//                    final BlockBuilder singleRowBlockBuilder = rowBuilder.beginBlockEntry();
//                    writeNativeValue(BIGINT, singleRowBlockBuilder, dataFile.columnSizes() != null ? dataFile.columnSizes().get(id) : null);
//                    writeNativeValue(BIGINT, singleRowBlockBuilder, dataFile.valueCounts() != null ? dataFile.valueCounts().get(id) : null);
//                    writeNativeValue(BIGINT, singleRowBlockBuilder, dataFile.nullValueCounts() != null ? dataFile.nullValueCounts().get(id) : null);
//
//                    // actual primitive Type for bounds
//                    final Type type = ((RowType) columnHandle.getType()).getFields().get(4).getType();
//                    org.apache.iceberg.types.Type icebergType = toIcebergType(type);
//                    if (icebergType.isPrimitiveType() && dataFile.lowerBounds() != null && dataFile.upperBounds() != null) {
//                        final Object lowerBound = convert(fromByteBuffer(icebergType, dataFile.lowerBounds().get(id)), icebergType);
//                        writeNativeValue(type, singleRowBlockBuilder, lowerBound);
//                        final Object upperBound = convert(fromByteBuffer(icebergType, dataFile.upperBounds().get(id)), icebergType);
//                        writeNativeValue(type, singleRowBlockBuilder, upperBound);
//                    }
//                    else {
//                        singleRowBlockBuilder.appendNull();
//                        singleRowBlockBuilder.appendNull();
//                    }
//                    rowBuilder.closeEntry();
//                }
//                else if (id == -1) { // indicates partition key
//                    final BlockBuilder blockBuilder = pageListBuilder.nextColumn();
//                    final org.apache.iceberg.types.Type icebergType = toIcebergType(columnHandle.getType());
//                    writeNativeValue(
//                            columnHandle.getType(),
//                            blockBuilder,
//                            convert(partition.get(partitionIndex, icebergType.typeId().javaClass()), icebergType));
//                    partitionIndex++;
//                }
//            }
            pageListBuilder.endRow();
        }

        this.readTimeNanos += System.nanoTime() - start;
        this.pages = pageListBuilder.build().iterator();
        return pages.next();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
    }

    private void writeArray(List<?> values, Type type, BlockBuilder blockBuilder)
    {
        if (values == null) {
            blockBuilder.appendNull();
        }
        else {
            BlockBuilder array = blockBuilder.beginBlockEntry();
            for (Object value : values) {
                writeNativeValue(type, array, value);
            }
            blockBuilder.closeEntry();
        }
    }

    private static class ColumnStats
    {
        private Long columnSize;
        private Long recordCount;
        private Long nullCount;
        private Object lowerBound;
        private Object upperBound;

        public ColumnStats(Long columnSize, Long recordCount, Long nullCount, Object lowerBound, Object upperBound)
        {
            this.columnSize = columnSize;
            this.recordCount = recordCount;
            this.nullCount = nullCount;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }
    }
}
