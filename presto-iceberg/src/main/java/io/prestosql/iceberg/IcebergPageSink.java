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

import com.google.common.primitives.Ints;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.iceberg.parquet.writer.PrestoWriteSupport;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.DateTimeEncoding;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.SqlVarbinary;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.TypeToMessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static io.prestosql.iceberg.MetricsParser.toJson;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.DATE;
import static io.prestosql.spi.type.StandardTypes.DECIMAL;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.REAL;
import static io.prestosql.spi.type.StandardTypes.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.StandardTypes.TINYINT;
import static io.prestosql.spi.type.StandardTypes.VARBINARY;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class IcebergPageSink
        implements ConnectorPageSink
{
    private Schema outputSchema;
    private PartitionSpec partitionSpec;
    private String outputDir;
    private Configuration configuration;
    private List<HiveColumnHandle> inputColumns;
    private List<HiveColumnHandle> partitionColumns;
    private Map<String, WriteContext> pathToWriter;
    private JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final TypeManager typeManager;
    private final FileFormat fileFormat;
    private final List<Type> partitionTypes;
    private final List<Boolean> isNullable;
    private static final TypeToMessageType typeToMessageType = new TypeToMessageType();

    public IcebergPageSink(Schema outputSchema,
            PartitionSpec partitionSpec,
            String outputDir,
            Configuration configuration,
            List<HiveColumnHandle> inputColumns,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            FileFormat fileFormat)
    {
        this.outputSchema = outputSchema;
        this.partitionSpec = partitionSpec;
        this.outputDir = outputDir;
        this.configuration = configuration;
        // TODO we only mark identity columns as partition columns as of now but we need to extract the schema on the coordinator side and provide partition
        // transforms in ConnectorOutputTableHandle
        this.partitionColumns = inputColumns.stream().filter(col -> col.isPartitionKey()).collect(toList());
        this.jsonCodec = jsonCodec;
        this.session = session;
        this.typeManager = typeManager;
        this.fileFormat = fileFormat;
        this.pathToWriter = new ConcurrentHashMap<>();
        this.partitionTypes = partitionColumns.stream().map(col -> typeManager.getType(col.getTypeSignature())).collect(toList());
        this.inputColumns = inputColumns;
        this.isNullable = outputSchema.columns().stream().map(column -> column.isOptional()).collect(toList());
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        int numRows = page.getPositionCount();
        Map<String, List<Integer>> partitionToPageRowNumber = new HashMap<>();

        for (int rowNum = 0; rowNum < numRows; rowNum++) {
            PartitionData partitionData = getPartitionData(session, page, rowNum);
            String partitionPath = partitionSpec.partitionToPath(partitionData);

            // TODO check if we really need to make this thread safe.
            if (!pathToWriter.containsKey(partitionPath)) {
                synchronized (pathToWriter) {
                    if (!pathToWriter.containsKey(partitionPath)) {
                        addWriter(partitionPath, partitionData);
                    }
                }
            }

            if (partitionToPageRowNumber.get(partitionPath) == null) {
                partitionToPageRowNumber.put(partitionPath, new ArrayList<>());
            }
            partitionToPageRowNumber.get(partitionPath).add(rowNum);
        }

        for (Map.Entry<String, WriteContext> partitionToWriteContext : pathToWriter.entrySet()) {
            final List<Integer> rowNums = partitionToPageRowNumber.get(partitionToWriteContext.getKey());
            final FileAppender<Page> writer = partitionToWriteContext.getValue().writer();
            Page partition = page.getPositions(Ints.toArray(rowNums), 0, rowNums.size());
            writer.add(partition);
        }

        return NOT_BLOCKED;
    }

    // implement getCompletedBytes, see ParquetRecordWriterUtil

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        Collection<Slice> commitTasks = new ArrayList<>();
        for (WriteContext writeContext : pathToWriter.values()) {
            String file = writeContext.fileLocation();
            final FileAppender<Page> fileAppender = writeContext.writer();
            try {
                fileAppender.close();
            }
            catch (IOException e) {
                abort();
                throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Failed to close" + file);
            }
            Metrics metrics = fileAppender.metrics();
            PartitionData partitionValue = writeContext.partitionValue();
            String partitionDataJson = partitionValue != null ? partitionValue.toJson() : null;
            commitTasks.add(Slices.wrappedBuffer(jsonCodec.toJsonBytes(new CommitTaskData(file, toJson(metrics), file, partitionDataJson))));
        }

        return CompletableFuture.completedFuture(commitTasks);
    }

    @Override
    public void abort()
    {
        pathToWriter.values().stream().forEach(writeContext -> {
            try {
                new Path(writeContext.fileLocation()).getFileSystem(configuration).delete(new Path(writeContext.fileLocation()), false);
            }
            catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        });
    }

    private void addWriter(String partitionPath, PartitionData partitionValue)
    {
        Path dataDir = new Path(outputDir);
        String pathUUId = randomUUID().toString(); // TODO add more context here instead of just random UUID, ip of the host, taskId
        Path outputPath = (partitionPath != null && !partitionPath.isEmpty()) ? new Path(new Path(dataDir, partitionPath), pathUUId) : new Path(dataDir, pathUUId);
        String outputFilePath = fileFormat.addExtension(outputPath.toString());
        OutputFile outputFile = HadoopOutputFile.fromPath(new Path(outputFilePath), configuration);
        switch (fileFormat) {
            case PARQUET:
                try {
                    FileAppender<Page> writer = Parquet.write(outputFile)
                            .schema(outputSchema)
                            .writeSupport(new PrestoWriteSupport(inputColumns, typeToMessageType.convert(outputSchema, "presto_schema"), outputSchema, typeManager, session, isNullable))
                            .build();
                    pathToWriter.put(partitionPath, new WriteContext(writer, outputFilePath, partitionValue));
                }
                catch (IOException e) {
                    throw new RuntimeIOException("Could not create writer ", e);
                }
            case ORC:
            case AVRO:
            default:
                throw new UnsupportedOperationException("Only parquet is supported for iceberg as of now but got fileformat " + fileFormat);
        }
    }

    private class PartitionWriteContext
    {
        private final List<Integer> rowNum;
        private final FileAppender<Page> writer;

        private PartitionWriteContext(List<Integer> rowNum, FileAppender<Page> writer)
        {
            this.rowNum = rowNum;
            this.writer = writer;
        }

        public List<Integer> getRowNum()
        {
            return rowNum;
        }

        public FileAppender<Page> getWriter()
        {
            return writer;
        }
    }

    private final PartitionData getPartitionData(ConnectorSession session, Page page, int rowNum)
    {
        // TODO only handles identity columns right now, handle all transforms
        Object[] values = new Object[partitionColumns.size()];
        for (int i = 0; i < partitionColumns.size(); i++) {
            HiveColumnHandle columnHandle = partitionColumns.get(i);
            Type type = partitionTypes.get(i);
            values[i] = getValue(session, page.getBlock(columnHandle.getHiveColumnIndex()), rowNum, type);
        }

        return new PartitionData(values);
    }

    public static final Object getValue(ConnectorSession session, Block block, int rownum, Type type)
    {
        if (block.isNull(rownum)) {
            return null;
        } // remove usage of getObjectValue use the actual getTYPE call.
        switch (type.getTypeSignature().getBase()) {
            case BIGINT:
                return type.getLong(block, rownum);
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return toIntExact(type.getLong(block, rownum));
            case BOOLEAN:
                return type.getBoolean(block, rownum);
            case DATE:
                return ((SqlDate) type.getObjectValue(session, block, rownum)).getDays();
            case DECIMAL:
                return ((SqlDecimal) type.getObjectValue(session, block, rownum)).toBigDecimal();
            case REAL:
                return intBitsToFloat((int) type.getLong(block, rownum));
            case DOUBLE:
                return type.getDouble(block, rownum);
            case TIMESTAMP:
                return MILLISECONDS.toMicros(type.getLong(block, rownum));
            case TIMESTAMP_WITH_TIME_ZONE:
                return MILLISECONDS.toMicros(unpackMillisUtc(type.getLong(block, rownum)));
            case VARBINARY:
                return ((SqlVarbinary) type.getObjectValue(session, block, rownum)).getBytes();
            case VARCHAR:
                return type.getObjectValue(session, block, rownum);
            default:
                throw new UnsupportedOperationException(type + " is not supported as partition column");
        }
    }

    private class WriteContext
    {
        private final FileAppender<Page> writer;
        private final String fileLocation;
        private final PartitionData partitionValue;

        public WriteContext(FileAppender<Page> writer, String fileLocation, PartitionData partitionValue)
        {
            this.writer = writer;
            this.fileLocation = fileLocation;
            this.partitionValue = partitionValue;
        }

        public FileAppender<Page> writer()
        {
            return writer;
        }

        public String fileLocation()
        {
            return fileLocation;
        }

        public PartitionData partitionValue()
        {
            return partitionValue;
        }
    }
}
