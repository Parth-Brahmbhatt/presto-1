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
package io.prestosql.plugin.hive.netflix;

import com.google.common.base.Splitter;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorSecurityContext;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.HiveSessionProperties.AWS_IAM_ROLE;
import static io.prestosql.spi.security.AccessDeniedException.denyDropTable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NetflixAccessControl
        implements ConnectorAccessControl
{
    private static final Splitter SPLITTER = Splitter.on('=').trimResults().omitEmptyStrings();

    private final HiveMetastore metastore;
    private List<String> s3RoleMappings;

    @Inject
    public NetflixAccessControl(HiveMetastore metastore, HiveConfig hiveConfig)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        requireNonNull(hiveConfig, "hiveConfig is null");
        this.s3RoleMappings = requireNonNull(hiveConfig.getS3RoleMappings(), "s3RoleMappings is null");
    }

    @Override
    public void checkCanCreateSchema(ConnectorSecurityContext context, String schemaName)
    {
    }

    @Override
    public void checkCanDropSchema(ConnectorSecurityContext context, String schemaName)
    {
    }

    @Override
    public void checkCanRenameSchema(ConnectorSecurityContext context, String schemaName, String newSchemaName)
    {
    }

    @Override
    public void checkCanShowSchemas(ConnectorSecurityContext context)
    {
    }

    @Override
    public Set<String> filterSchemas(ConnectorSecurityContext context, Set<String> schemaNames)
    {
        return schemaNames;
    }

// @todo(anjali) This function does not exist in parent class
//    @Override
//    public void checkCanShowTablesMetadata(ConnectorSecurityContext context, String schemaName)
//    {
//    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorSecurityContext context, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkAccess(context.getIdentity(), tableName);
    }

    @Override
    public void checkCanDropTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        Optional<Table> target = metastore.getTable(new HiveIdentity(context.getIdentity()), tableName.getSchemaName(), tableName.getTableName());

        if (!target.isPresent()) {
            denyDropTable(tableName.toString(), "Table not found");
        }
        checkAccess(context.getIdentity(), tableName);
    }

    @Override
    public void checkCanRenameTable(ConnectorSecurityContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
    }

    @Override
    public void checkCanAddColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkAccess(context.getIdentity(), tableName);
    }

    @Override
    public void checkCanDropColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkAccess(context.getIdentity(), tableName);
    }

    @Override
    public void checkCanRenameColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkAccess(context.getIdentity(), tableName);
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> colunmNames)
    {
        checkAccess(context.getIdentity(), tableName);
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkAccess(context.getIdentity(), tableName);
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkAccess(context.getIdentity(), tableName);
    }

    @Override
    public void checkCanCreateView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        checkAccess(context.getIdentity(), viewName);
    }

    @Override
    public void checkCanDropView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        checkAccess(context.getIdentity(), viewName);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorSecurityContext context, SchemaTableName viewName, Set<String> columnNames)
    {
        checkAccess(context.getIdentity(), viewName);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorSecurityContext context, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
        checkAccess(context.getIdentity(), tableName);
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        checkAccess(context.getIdentity(), tableName);
    }

    public void checkCanShowColumnsMetadata(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkAccess(context.getIdentity(), tableName);
    }

    /**
     * Filter the list of columns to those visible to the identity.
     */
    public List<ColumnMetadata> filterColumns(ConnectorSecurityContext context, SchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return columns;
    }

    private void checkAccess(ConnectorIdentity identity, SchemaTableName tableName)
    {
        for (String roleMapping : this.s3RoleMappings) {
            List<String> splitted = SPLITTER.splitToList(roleMapping);
            checkArgument(splitted.size() == 2, "Splitted s3 role mapping should have two elements (e.g., schema=role)");
            String schema = splitted.get(0);
            String roleName = splitted.get(1);
            // the user is accessing a schema that has a configured role mapping
            if (Objects.equals(tableName.getSchemaName(), schema)) {
                Map<String, String> sessionProperties = identity.getSessionProperties();

                if (sessionProperties == null || sessionProperties.isEmpty()) {
                    denyAccess(schema);
                }

                Optional<Map.Entry<String, String>> roleProperty = sessionProperties
                        .entrySet()
                        .stream()
                        .filter(entry -> entry.getKey().equalsIgnoreCase(AWS_IAM_ROLE + "_" + schema)).findAny();

                if (roleProperty.isPresent() == false) {
                    roleProperty = sessionProperties
                            .entrySet()
                            .stream()
                            .filter(entry -> entry.getKey().equalsIgnoreCase(AWS_IAM_ROLE)).findAny();
                }

                if (!roleProperty.isPresent()) {
                    denyAccess(schema);
                }

                String sessionRoleName = roleProperty.get().getValue();
                if (!Objects.equals(roleName, sessionRoleName)) {
                    denyAccess(schema);
                }
            }
        }
    }

    private void denyAccess(String schema)
    {
        throw new AccessDeniedException(format("To access this table you should specify the role arn for %s with the %s session property " +
                        "on the catalog that you are accessing: \"set session ${catalog_name, i.e. prodhive/testhive/iceberg}.%s=<%s role_arn>\"",
                schema, AWS_IAM_ROLE, AWS_IAM_ROLE, schema));
    }
}
