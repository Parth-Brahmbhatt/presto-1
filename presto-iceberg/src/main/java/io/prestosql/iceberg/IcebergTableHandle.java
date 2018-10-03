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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.ConnectorTableHandle;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IcebergTableHandle
        implements ConnectorTableHandle
{
    private static final Pattern TABLE_PATTERN = Pattern.compile(
            "(?<table>[^$@]+)(?:@(?<ver1>[^$]*))?(?:\\$(?<type>[^@]*)(?:@(?<ver2>.*))?)?");
    private static final Logger LOG = Logger.get(IcebergTableHandle.class);

    private final String schemaName;
    private final String tableName;
    private final TableType tableType;
    private final Long atId;

    public IcebergTableHandle(@JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") TableType tableType,
            @JsonProperty("atId") Long atId)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.atId = atId;
        this.tableType = tableType;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public Long getAtId()
    {
        return atId;
    }

    public static IcebergTableHandle parse(String tableIdentifier, String schemaName)
    {
        Matcher match = TABLE_PATTERN.matcher(tableIdentifier);
        if (match.matches()) {
            try {
                String table = match.group("table");
                String typeStr = match.group("type");
                String ver1 = match.group("ver1");
                String ver2 = match.group("ver2");

                TableType type;
                if (typeStr != null) {
                    type = TableType.valueOf(typeStr.toUpperCase(Locale.ROOT));
                }
                else {
                    type = TableType.DATA;
                }

                Long version;
                if (type == TableType.DATA ||
                        type == TableType.PARTITIONS ||
                        type == TableType.MANIFESTS) {
                    Preconditions.checkArgument(ver1 == null || ver2 == null,
                            "Cannot specify two @ versions");
                    if (ver1 != null) {
                        version = Long.parseLong(ver1);
                    }
                    else if (ver2 != null) {
                        version = Long.parseLong(ver2);
                    }
                    else {
                        version = null;
                    }
                }
                else {
                    Preconditions.checkArgument(ver1 == null && ver2 == null,
                            "Cannot use @ version with table type %s: %s", typeStr, tableIdentifier);
                    version = null;
                }

                return new IcebergTableHandle(schemaName, table, type, version);
            }
            catch (IllegalArgumentException e) {
                LOG.warn("Failed to parse table name, using {}: {}", tableIdentifier, e.getMessage());
            }
        }

        return new IcebergTableHandle(schemaName, tableIdentifier, TableType.DATA, null);
    }
}
