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
package io.prestosql.plugin.jdbc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class JdbcMetadataConfig
{
    private boolean allowDropTable;
    private boolean allowAggregationPushdown = true;
    private boolean allowTopNPushdown = true;

    public boolean isAllowDropTable()
    {
        return allowDropTable;
    }

    @Config("allow-drop-table")
    @ConfigDescription("Allow connector to drop tables")
    public JdbcMetadataConfig setAllowDropTable(boolean allowDropTable)
    {
        this.allowDropTable = allowDropTable;
        return this;
    }

    public boolean isAllowAggregationPushdown()
    {
        return allowAggregationPushdown;
    }

    @Config("allow-aggregation-pushdown")
    @ConfigDescription("Allow aggregation pushdown")
    public JdbcMetadataConfig setAllowAggregationPushdown(boolean allowAggregationPushdown)
    {
        this.allowAggregationPushdown = allowAggregationPushdown;
        return this;
    }

    @Config("allow-topn-pushdown")
    @ConfigDescription("Allow TopN pushdown")
    public JdbcMetadataConfig setAllowTopNPushdown(boolean allowTopNPushdown)
    {
        this.allowTopNPushdown = allowTopNPushdown;
        return this;
    }

    public Boolean isAllowTopNPushdown()
    {
        return this.allowTopNPushdown;
    }
}
