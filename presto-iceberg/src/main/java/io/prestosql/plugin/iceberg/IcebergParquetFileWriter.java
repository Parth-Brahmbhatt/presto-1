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

import io.prestosql.parquet.writer.ParquetWriterOptions;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.parquet.ParquetFileWriter;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

public class IcebergParquetFileWriter
        extends ParquetFileWriter
        implements IcebergFileWriter
{
    private final Path outputPath;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;

    public IcebergParquetFileWriter(
            OutputStream outputStream,
            Callable<Void> rollbackAction,
            List<Type> fileColumnTypes,
            MessageType messageType,
            Map<List<String>, Type> primitiveTypes,
            ParquetWriterOptions parquetWriterOptions,
            int[] fileInputColumnIndexes,
            CompressionCodecName compressionCodecName,
            Path outputPath,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext)
    {
        super(outputStream,
                rollbackAction,
                fileColumnTypes,
                messageType,
                primitiveTypes,
                parquetWriterOptions,
                fileInputColumnIndexes,
                compressionCodecName);
        this.outputPath = requireNonNull(outputPath, "outputPath is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
    }

    @Override
    public Metrics getMetrics()
    {
        // anjali BIG HACK because of the metrics issue
        try {
            return hdfsEnvironment.doAs(hdfsContext.getIdentity().getUser(), () -> ParquetUtil.fileMetrics(new HdfsInputFile(outputPath, hdfsEnvironment, hdfsContext), MetricsConfig.getDefault()));
        }
        catch (Exception e) {
           // if (hdfsEnvironment.getConfiguration(hdfsContext, new Path("file://tmp")).get("ignore.iceberg.stats", "false").equals("true")) {
            return new Metrics((long) 1, new HashMap<>(), new HashMap<>(), new HashMap<>());
           // }
           // throw e;
        }
    }
}
