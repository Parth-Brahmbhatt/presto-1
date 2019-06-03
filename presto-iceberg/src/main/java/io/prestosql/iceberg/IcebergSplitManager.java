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

import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorSplitSource;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;

import javax.inject.Inject;

public class IcebergSplitManager
        implements ConnectorSplitManager
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeTranslator typeTranslator;
    private final TypeManager typeRegistry;
    private final IcebergUtil icebergUtil;

    @Inject
    public IcebergSplitManager(
            HdfsEnvironment hdfsEnvironment,
            TypeTranslator typeTranslator,
            TypeManager typeRegistry,
            IcebergUtil icebergUtil)
    {
        this.hdfsEnvironment = hdfsEnvironment;
        this.typeTranslator = typeTranslator;
        this.typeRegistry = typeRegistry;
        this.icebergUtil = icebergUtil;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingStrategy splitSchedulingStrategy)
    {
        IcebergTableLayoutHandle tbl = (IcebergTableLayoutHandle) layout;

        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session, tbl.getDatabase()), new Path("file:///tmp"));
        Table icebergTable = icebergUtil.getIcebergTable(tbl.getDatabase(), tbl.getTableName(), configuration);

        TableScan tableScan = icebergUtil.getTableScan(session, tbl.getPredicates(), tbl.getAtId(), icebergTable);

        // TODO Use residual. Right now there is no way to propagate residual to presto but at least we can
        // propagate it at split level so the parquet pushdown can leverage it.
        IcebergSplitSource icebergSplitSource = new IcebergSplitSource(
                tbl.getDatabase(),
                tbl.getTableName(),
                tableScan.planTasks().iterator(),
                tbl.getPredicates(),
                session,
                icebergTable.schema(),
                hdfsEnvironment,
                typeTranslator,
                typeRegistry,
                tbl.getNameToColumnHandle());
        return new ClassLoaderSafeConnectorSplitSource(icebergSplitSource, Thread.currentThread().getContextClassLoader());
    }
}
