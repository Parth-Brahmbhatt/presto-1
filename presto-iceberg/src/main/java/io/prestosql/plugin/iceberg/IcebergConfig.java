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

import io.airlift.configuration.Config;
import io.prestosql.plugin.hive.HiveCompressionCodec;
import org.apache.iceberg.FileFormat;

import javax.validation.constraints.NotNull;

import static io.prestosql.plugin.hive.HiveCompressionCodec.GZIP;
import static io.prestosql.plugin.iceberg.IcebergFileFormat.PARQUET;

public class IcebergConfig
{
    private IcebergFileFormat fileFormat = PARQUET;
    private HiveCompressionCodec compressionCodec = GZIP;
    private String metastoreRestEndpoint;
    private String metacatCatalogName;
    private String metastoreWarehoseDir;

    @NotNull
    public FileFormat getFileFormat()
    {
        return FileFormat.valueOf(fileFormat.name());
    }

    @Config("iceberg.file-format")
    public IcebergConfig setFileFormat(IcebergFileFormat fileFormat)
    {
        this.fileFormat = fileFormat;
        return this;
    }

    @NotNull
    public HiveCompressionCodec getCompressionCodec()
    {
        return compressionCodec;
    }

    @Config("iceberg.compression-codec")
    public IcebergConfig setCompressionCodec(HiveCompressionCodec compressionCodec)
    {
        this.compressionCodec = compressionCodec;
        return this;
    }

    public String getMetastoreRestEndpoint()
    {
        return metastoreRestEndpoint;
    }

    @Config("iceberg.metacat-rest-endpoint")
    public void setMetastoreRestEndpoint(String metastoreRestEndpoint)
    {
        this.metastoreRestEndpoint = metastoreRestEndpoint;
    }

    public String getMetacatCatalogName()
    {
        return metacatCatalogName;
    }

    @Config("iceberg.metacat-catalog-name")
    public void setMetacatCatalogName(String metacatCatalogName)
    {
        this.metacatCatalogName = metacatCatalogName;
    }

    @Config("iceberg.metastore-warehouse-dir")
    public void setMetastoreWarehoseDir(String metastoreWarehoseDir)
    {
        this.metastoreWarehoseDir = metastoreWarehoseDir;
    }

    public String getMetastoreWarehoseDir()
    {
        return metastoreWarehoseDir;
    }
}
