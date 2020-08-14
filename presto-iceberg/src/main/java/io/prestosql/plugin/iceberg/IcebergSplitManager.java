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
import io.prestosql.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.FixedSplitSource;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;

import javax.inject.Inject;

import static io.prestosql.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.prestosql.plugin.iceberg.IcebergUtil.getIcebergTable;
import static java.util.Objects.requireNonNull;

public class IcebergSplitManager
        implements ConnectorSplitManager
{
    public static final int ICEBERG_DOMAIN_COMPACTION_THRESHOLD = 1000;

    private final IcebergTransactionManager transactionManager;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public IcebergSplitManager(IcebergTransactionManager transactionManager, HdfsEnvironment hdfsEnvironment)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle handle,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;

        if (table.getSnapshotId().isEmpty()) {
            return new FixedSplitSource(ImmutableList.of());
        }

        HiveMetastore metastore = transactionManager.get(transaction).getMetastore();
        Table icebergTable = getIcebergTable(metastore, hdfsEnvironment, session, table);

        TableScan tableScan = icebergTable.newScan()
                .filter(toIcebergExpression(
                        table.getEnforcedPredicate()
                                // TODO: Remove TupleDomain#simplify once Iceberg supports IN expression. Currently this
                                // is required for IN predicates on non-partition columns with large value list. Such
                                // predicates on partition columns are not supported.
                                // (See AbstractTestIcebergSmoke#testLargeInFailureOnPartitionedColumns)
                                .intersect(table.getUnenforcedPredicate().simplify(ICEBERG_DOMAIN_COMPACTION_THRESHOLD))))
                .useSnapshot(table.getSnapshotId().get());

        // TODO Use residual. Right now there is no way to propagate residual to presto but at least we can
        //      propagate it at split level so the parquet pushdown can leverage it.
        IcebergSplitSource splitSource = new IcebergSplitSource(tableScan.planTasks());

        return new ClassLoaderSafeConnectorSplitSource(splitSource, Thread.currentThread().getContextClassLoader());
    }
}
