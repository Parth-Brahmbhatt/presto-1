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

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorSplitSource;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableScan;
import com.netflix.iceberg.expressions.Expression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.IcebergUtil.SNAPSHOT_ID;
import static com.facebook.presto.iceberg.IcebergUtil.SNAPSHOT_TIMESTAMP_MS;

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
        final IcebergTableLayoutHandle tbl = (IcebergTableLayoutHandle) layout;
        final TupleDomain<HiveColumnHandle> predicates = tbl.getPredicates().getDomains()
                .map(m -> m.entrySet().stream().collect(Collectors.toMap((x) -> HiveColumnHandle.class.cast(x.getKey()), Map.Entry::getValue)))
                .map(m -> TupleDomain.withColumnDomains(m)).orElse(TupleDomain.none());
        final Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session, tbl.getDatabase()), new Path("file:///tmp"));
        final Table icebergTable = icebergUtil.getIcebergTable(tbl.getDatabase(), tbl.getTableName(), configuration);
        final Long snapshotId = getPredicateValue(predicates, SNAPSHOT_ID);
        final Long snapshotTimestamp = getPredicateValue(predicates, SNAPSHOT_TIMESTAMP_MS);
        final Expression expression = ExpressionConverter.toIceberg(predicates, session);
        TableScan tableScan = icebergTable.newScan().filter(expression);

        if (snapshotId != null && snapshotTimestamp != null) {
            throw new IllegalArgumentException(String.format("Either specify a predicate on %s or %s but not both", SNAPSHOT_ID, SNAPSHOT_TIMESTAMP_MS));
        }

        if (snapshotId != null) {
            tableScan = tableScan.useSnapshot(snapshotId);
        }
        else if (snapshotTimestamp != null) {
            tableScan = tableScan.asOfTime(snapshotTimestamp);
        }

        // TODO Use residual Right now there is no way to propagate residual to presto but at least we can
        // propagate it at split level so the parquet pushdown can leverage it.
        final IcebergSplitSource icebergSplitSource = new IcebergSplitSource(
                tbl.getDatabase(),
                tbl.getTableName(),
                tableScan.planTasks().iterator(),
                predicates,
                session,
                icebergTable.schema(),
                hdfsEnvironment,
                typeTranslator,
                typeRegistry,
                tbl.getNameToColumnHandle(),
                snapshotId,
                snapshotTimestamp);
        return new ClassLoaderSafeConnectorSplitSource(icebergSplitSource, Thread.currentThread().getContextClassLoader());
    }

    private Long getPredicateValue(TupleDomain<HiveColumnHandle> predicates, String columnName)
    {
        if (predicates.isNone() || predicates.isAll()) {
            return null;
        }

        return predicates.getDomains().map(hiveColumnHandleDomainMap -> {
            final List<Domain> snapShotDomains = hiveColumnHandleDomainMap.entrySet().stream()
                    .filter(hiveColumnHandleDomainEntry -> hiveColumnHandleDomainEntry.getKey().getName().equals(columnName))
                    .map(hiveColumnHandleDomainEntry -> hiveColumnHandleDomainEntry.getValue())
                    .collect(Collectors.toList());

            if (snapShotDomains.isEmpty()) {
                return null;
            }

            if (snapShotDomains.size() > 1 || !snapShotDomains.get(0).isSingleValue()) {
                throw new IllegalArgumentException(String.format("Only %s = value check is allowed on column = %s", columnName, columnName));
            }

            return (Long) snapShotDomains.get(0).getSingleValue();
        }).orElse(null);
    }
}
