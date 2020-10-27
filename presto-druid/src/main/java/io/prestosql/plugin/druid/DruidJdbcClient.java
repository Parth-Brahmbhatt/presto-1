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
package io.prestosql.plugin.druid;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.prestosql.plugin.druid.aggregate.SingleInputAggregateFunction;
import io.prestosql.plugin.druid.function.FunctionRuleDSL;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.QueryBuilder;
import io.prestosql.plugin.jdbc.RemoteTableName;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRule;
import io.prestosql.plugin.jdbc.expression.FunctionRewriter;
import io.prestosql.plugin.jdbc.expression.FunctionRule;
import io.prestosql.plugin.jdbc.expression.ImplementCount;
import io.prestosql.plugin.jdbc.expression.ImplementCountAll;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.expression.ArithmaticBinaryExpression;
import io.prestosql.spi.expression.CaseExpression;
import io.prestosql.spi.expression.Cast;
import io.prestosql.spi.expression.CoalesceExpression;
import io.prestosql.spi.expression.ComparisonExpression;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.Constant;
import io.prestosql.spi.expression.FunctionCall;
import io.prestosql.spi.expression.InExpression;
import io.prestosql.spi.expression.InListExpression;
import io.prestosql.spi.expression.Operator;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.expression.WhenClause;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.prestoTypeToJdbcType;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.util.stream.Collectors.toList;

public class DruidJdbcClient
        extends BaseJdbcClient
{
    // Druid maintains its datasources related metadata by setting the catalog name as "druid"
    // Note that while a user may name the catalog name as something else, metadata queries made
    // to druid will always have the TABLE_CATALOG set to DRUID_CATALOG
    private static final String DRUID_CATALOG = "druid";
    // All the datasources in Druid are created under schema "druid"
    public static final String DRUID_SCHEMA = "druid";
    private AggregateFunctionRewriter aggregateFunctionRewriter;
    private FunctionRewriter functionRewriter;

    @Inject
    public DruidJdbcClient(BaseJdbcConfig config, ConnectionFactory connectionFactory)
    {
        super(config, "\"", connectionFactory);

        this.aggregateFunctionRewriter = new AggregateFunctionRewriter(
                this::quoted,
                aggregateFunctionRules());

        this.functionRewriter = new FunctionRewriter(
                this::quoted,
                functionRules());
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        return ImmutableList.of(DRUID_SCHEMA);
    }

    @Override
    public Optional<JdbcExpression> handleConnectorExpression(ConnectorSession session, ConnectorExpression connectorExpression, Map<String, ColumnHandle> assignments)
    {
        final Map<String, JdbcColumnHandle> columnHandleMap = assignments.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, column -> (JdbcColumnHandle) column.getValue()));

        final FunctionRule.RewriteContext context = functionRewriter.getContext(session, columnHandleMap);
        return processConnectorExpression(connectorExpression, context, true);
    }

    public Optional<JdbcExpression> processConnectorExpression(ConnectorExpression connectorExpression, FunctionRule.RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        if (connectorExpression instanceof FunctionCall) {
            return processConnectorExpression((FunctionCall) connectorExpression, context, shouldQuoteStringLiterals);
        }
        else if (connectorExpression instanceof Variable) {
            return processConnectorExpression((Variable) connectorExpression, context, shouldQuoteStringLiterals);
        }
        else if (connectorExpression instanceof Constant) {
            return processConnectorExpression((Constant) connectorExpression, context, shouldQuoteStringLiterals);
        }
        else if (connectorExpression instanceof Cast) {
            return processConnectorExpression((Cast) connectorExpression, context, shouldQuoteStringLiterals);
        }
        else if (connectorExpression instanceof ComparisonExpression) {
            return processConnectorExpression((ComparisonExpression) connectorExpression, context, shouldQuoteStringLiterals);
        }
        else if (connectorExpression instanceof WhenClause) {
            return processConnectorExpression((WhenClause) connectorExpression, context, shouldQuoteStringLiterals);
        }
        else if (connectorExpression instanceof CaseExpression) {
            return processConnectorExpression((CaseExpression) connectorExpression, context, shouldQuoteStringLiterals);
        }
        else if (connectorExpression instanceof InExpression) {
            return processConnectorExpression((InExpression) connectorExpression, context, shouldQuoteStringLiterals);
        }
        else if (connectorExpression instanceof InListExpression) {
            return processConnectorExpression((InListExpression) connectorExpression, context, shouldQuoteStringLiterals);
        }
        else if (connectorExpression instanceof ArithmaticBinaryExpression) {
            return processConnectorExpression((ArithmaticBinaryExpression) connectorExpression, context, shouldQuoteStringLiterals);
        }
        else if (connectorExpression instanceof CoalesceExpression) {
            return processConnectorExpression((CoalesceExpression) connectorExpression, context, shouldQuoteStringLiterals);
        }
        return Optional.empty();
    }

    public Optional<JdbcExpression> processConnectorExpression(FunctionCall function, FunctionRule.RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        for (FunctionRule rule : functionRules()) {
            if (rule.getPattern().matches(function, context)) {
                final Optional<String> expressionFormat = rule.expressionFormat();
                final JdbcTypeHandle jdbcTypeHandle = rule.getJdbcTypeHandle(function.getType());
                String expression = expressionFormat.orElse(rule.getPrestoName() + "(%s)");
                final List<String> arguments = function.getArguments().stream()
                        .map(arg -> processConnectorExpression(arg, context, rule.shouldQuoteStringLiterals()))
                        .filter(Optional::isPresent)
                        .map(arg -> arg.get().getExpression())
                        .collect(toList());

                if (arguments.size() != function.getArguments().size()) {
                    continue;
                }
                expression = String.format(expression, Joiner.on(",").join(arguments));

                return Optional.of(new JdbcExpression(expression, jdbcTypeHandle));
            }
        }
        return Optional.empty();
    }

    private Optional<JdbcExpression> processConnectorExpression(Variable variable, FunctionRule.RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        return Optional.of(new JdbcExpression(
                context.getAssignments().get(variable.getName()).toSqlExpression(name -> context.getIdentifierQuote().apply(name)),
                context.getAssignments().get(variable.getName()).getJdbcTypeHandle()));
    }

    public Optional<JdbcExpression> processConnectorExpression(Constant constant, FunctionRule.RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        // TODO can not handle nulls as druid does not have a way to just select NULL
        if (constant.getValue() == null) {
            return Optional.empty();
        }

        String value;
        int size = 0;
        if (constant.getType() instanceof VarcharType) {
            size = ((VarcharType) constant.getType()).getLength().orElse(VarcharType.UNBOUNDED_LENGTH);
            if (shouldQuoteStringLiterals) {
                value = String.format("'%s'", ((Slice) constant.getValue()).toStringUtf8());
            }
            else {
                value = ((Slice) constant.getValue()).toStringUtf8();
            }
        }
        else {
            value = String.format("CAST(%s as %s)", constant.getValue(), JDBCType.valueOf(prestoTypeToJdbcType(constant.getType()).get()).getName());
        }

        return Optional.of(new JdbcExpression(
                value,
                new JdbcTypeHandle(prestoTypeToJdbcType(constant.getType()).get(), Optional.empty(), size, 0, Optional.empty(), Optional.empty())));
    }

    public Optional<JdbcExpression> processConnectorExpression(Cast cast, FunctionRule.RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        final Optional<JdbcExpression> expression = processConnectorExpression(cast.getExpression(), context, shouldQuoteStringLiterals);
        if (expression.isEmpty()) {
            return Optional.empty();
        }
        final Optional<Integer> jdbcType = prestoTypeToJdbcType(cast.getType());

        if (jdbcType.isEmpty() || (jdbcType.get() == JDBCType.NULL.getVendorTypeNumber())) {
            return Optional.empty();
        }

        String jdbcExpression = String.format("CAST(%s as %s)", expression.get().getExpression(), JDBCType.valueOf(jdbcType.get()).getName());
        int size = 0;
        if (cast.getType() instanceof VarcharType) {
            size = ((VarcharType) cast.getType()).getLength().orElse(VarcharType.UNBOUNDED_LENGTH);
        }

        return Optional.of(new JdbcExpression(
                jdbcExpression,
                new JdbcTypeHandle(jdbcType.get(), Optional.empty(), size, 0, Optional.empty(), Optional.empty())));
    }

    public Optional<JdbcExpression> processConnectorExpression(ComparisonExpression comparisonExpression, FunctionRule.RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        final Optional<JdbcExpression> left = processConnectorExpression(comparisonExpression.getLeft(), context, shouldQuoteStringLiterals);
        final Optional<JdbcExpression> right = processConnectorExpression(comparisonExpression.getRight(), context, shouldQuoteStringLiterals);
        final Optional<String> operator = processOperator(comparisonExpression.getOperator());
        if (left.isEmpty() || right.isEmpty() || operator.isEmpty()) {
            return Optional.empty();
        }

        String jdbcExpression = String.format("%s %s %s", left.get().getExpression(), operator.get(), right.get().getExpression());

        return Optional.of(new JdbcExpression(
                jdbcExpression,
                new JdbcTypeHandle(JDBCType.BOOLEAN.getVendorTypeNumber(), Optional.empty(), 0, 0, Optional.empty(), Optional.empty())));
    }

    public Optional<JdbcExpression> processConnectorExpression(WhenClause whenClause, FunctionRule.RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        final Optional<JdbcExpression> operand = processConnectorExpression(whenClause.getOperand(), context, shouldQuoteStringLiterals);
        final Optional<JdbcExpression> result = processConnectorExpression(whenClause.getResult(), context, shouldQuoteStringLiterals);
        if (operand.isEmpty() || result.isEmpty()) {
            return Optional.empty();
        }

        String jdbcExpression = String.format("WHEN %s THEN %s", operand.get().getExpression(), result.get().getExpression());

        return Optional.of(new JdbcExpression(
                jdbcExpression,
                new JdbcTypeHandle(prestoTypeToJdbcType(whenClause.getType()).get(), Optional.empty(), 0, 0, Optional.empty(), Optional.empty())));
    }

    public Optional<JdbcExpression> processConnectorExpression(CaseExpression caseExpression, FunctionRule.RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        Optional<JdbcExpression> defaultValue = Optional.empty();
        if (caseExpression.getDefaultValue().isPresent()) {
            defaultValue = processConnectorExpression(caseExpression.getDefaultValue().get(), context, shouldQuoteStringLiterals);
            if (defaultValue.isEmpty()) {
                return Optional.empty();
            }
        }

        Optional<JdbcExpression> operand = Optional.empty();
        if (caseExpression.getOperand().isPresent()) {
            operand = processConnectorExpression(caseExpression.getOperand().get(), context, shouldQuoteStringLiterals);
            if (operand.isEmpty()) {
                return Optional.empty();
            }
        }

        final List<String> whenClauses = caseExpression.getWhenClauses().stream()
                .map(when -> processConnectorExpression(when, context, shouldQuoteStringLiterals))
                .filter(Optional::isPresent)
                .map(when -> when.get().getExpression())
                .collect(toList());

        if (whenClauses.size() != caseExpression.getWhenClauses().size()) {
            return Optional.empty();
        }

        String operandExpression = operand.map(JdbcExpression::getExpression).orElse("");
        String whenExpression = String.join("\n", whenClauses);
        String defaultExpression = defaultValue.map(val -> String.format(" ELSE %s", val.getExpression())).orElse("");
        String jdbcExpression = String.format("CASE %s %s %s END", operandExpression, whenExpression, defaultExpression);

        return Optional.of(new JdbcExpression(
                jdbcExpression,
                new JdbcTypeHandle(prestoTypeToJdbcType(caseExpression.getType()).get(), Optional.empty(), 0, 0, Optional.empty(), Optional.empty())));
    }

    public Optional<JdbcExpression> processConnectorExpression(InExpression inExpression, FunctionRule.RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        final Optional<JdbcExpression> value = processConnectorExpression(inExpression.getValue(), context, shouldQuoteStringLiterals);
        final Optional<JdbcExpression> valueList = processConnectorExpression(inExpression.getValueList(), context, shouldQuoteStringLiterals);
        if (value.isEmpty() || valueList.isEmpty()) {
            return Optional.empty();
        }

        String jdbcExpression = String.format("%s IN (%s)", value.get().getExpression(), valueList.get().getExpression());

        return Optional.of(new JdbcExpression(
                jdbcExpression,
                new JdbcTypeHandle(JDBCType.BOOLEAN.getVendorTypeNumber(), Optional.empty(), 0, 0, Optional.empty(), Optional.empty())));
    }

    public Optional<JdbcExpression> processConnectorExpression(InListExpression inListExpression, FunctionRule.RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        final List<JdbcExpression> inExpressions = inListExpression.getValues().stream()
                .map(value -> processConnectorExpression(value, context, shouldQuoteStringLiterals))
                .filter(x -> x.isPresent())
                .map(Optional::get)
                .collect(toList());

        if (inExpressions.size() != inListExpression.getValues().size()) {
            return Optional.empty();
        }
        final String jdbcExpression = inExpressions.stream()
                .map(JdbcExpression::getExpression)
                .collect(Collectors.joining(","));

        return Optional.of(new JdbcExpression(
                jdbcExpression,
                new JdbcTypeHandle(JDBCType.BOOLEAN.getVendorTypeNumber(), Optional.empty(), 0, 0, Optional.empty(), Optional.empty())));
    }

    public Optional<JdbcExpression> processConnectorExpression(ArithmaticBinaryExpression arithmaticBinaryExpression, FunctionRule.RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        final Optional<String> operator = processOperator(arithmaticBinaryExpression.getOperator());
        final Optional<JdbcExpression> left = processConnectorExpression(arithmaticBinaryExpression.getLeft(), context, shouldQuoteStringLiterals);
        final Optional<JdbcExpression> right = processConnectorExpression(arithmaticBinaryExpression.getRight(), context, shouldQuoteStringLiterals);

        if (operator.isEmpty() || left == null || right == null || left.isEmpty() || right.isEmpty()) {
            return Optional.empty();
        }

        final String jdbcExpression = String.format("%s %s %s", left.get().getExpression(), operator.get(), right.get().getExpression());
        return Optional.of(new JdbcExpression(
                jdbcExpression,
                new JdbcTypeHandle(JDBCType.BOOLEAN.getVendorTypeNumber(), Optional.empty(), 0, 0, Optional.empty(), Optional.empty())));
    }

    public Optional<JdbcExpression> processConnectorExpression(CoalesceExpression coalesceExpression, FunctionRule.RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        List<JdbcExpression> connectorExpressions = coalesceExpression.getConnectorExpressions().stream()
                .map(expr -> processConnectorExpression(expr, context, shouldQuoteStringLiterals))
                .filter(expr -> expr != null && expr.isPresent())
                .map(Optional::get)
                .collect(Collectors.toList());

        if (connectorExpressions == null || connectorExpressions.size() != coalesceExpression.getConnectorExpressions().size()) {
            return Optional.empty();
        }

        final String coaleceList = connectorExpressions.stream()
                .map(expr -> expr.getExpression())
                .collect(Collectors.joining(","));

        final String jdbcExpression = String.format("COALESCE(%s)", coaleceList);
        final Optional<Integer> jdbcType = prestoTypeToJdbcType(coalesceExpression.getType());
        if (jdbcType.isEmpty()) {
            return Optional.empty();
        }
        int size = 0;
        if (coalesceExpression.getType() instanceof VarcharType) {
            size = ((VarcharType) coalesceExpression.getType()).getLength().orElse(VarcharType.UNBOUNDED_LENGTH);
        }
        return Optional.of(new JdbcExpression(
                jdbcExpression,
                new JdbcTypeHandle(jdbcType.get(), Optional.empty(), size, 0, Optional.empty(), Optional.empty())));
    }

    public Optional<String> processOperator(Operator operator)
    {
        switch (operator) {
            case EQUAL:
                return Optional.of("=");
            case NOT_EQUAL:
                return Optional.of("<>");
            case LESS_THAN:
                return Optional.of("<");
            case LESS_THAN_OR_EQUAL:
                return Optional.of("<=");
            case GREATER_THAN:
                return Optional.of(">");
            case GREATER_THAN_OR_EQUAL:
                return Optional.of(">=");
            default:
                return Optional.empty();
        }
    }

    public Optional<String> processOperator(ArithmaticBinaryExpression.Operator operator)
    {
        switch (operator) {
            case ADD:
                return Optional.of("+");
            case SUBTRACT:
                return Optional.of("-");
            case MULTIPLY:
                return Optional.of("*");
            case DIVIDE:
                return Optional.of("/");
            case MODULUS:
                return Optional.of("%");
        }
        return Optional.empty();
    }

    //Overridden to filter out tables that don't match schemaTableName
    @Override
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            try (ResultSet resultSet = getTables(connection, Optional.of(jdbcSchemaName), Optional.of(jdbcTableName))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            schemaTableName,
                            DRUID_CATALOG,
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(
                        getOnlyElement(
                                tableHandles
                                        .stream()
                                        .filter(
                                                jdbcTableHandle ->
                                                        Objects.equals(jdbcTableHandle.getSchemaName(), schemaTableName.getSchemaName())
                                                                && Objects.equals(jdbcTableHandle.getTableName(), schemaTableName.getTableName()))
                                        .collect(Collectors.toList())));
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /*
     * Overridden since the {@link BaseJdbcClient#getTables(Connection, Optional, Optional)}
     * method uses character escaping that doesn't work well with Druid's Avatica handler.
     * Unfortunately, because we can't escape search characters like '_' and '%", this call
     * ends up retrieving metadata for all tables that match the search
     * pattern. For ex - LIKE some_table matches somertable, somextable and some_table.
     *
     * See getTableHandle(JdbcIdentity, SchemaTableName)} to look at
     * how tables are filtered.
     */
    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(DRUID_CATALOG,
                DRUID_SCHEMA,
                tableName.orElse(null),
                null);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        switch (typeHandle.getJdbcType()) {
            case Types.VARCHAR:
                int columnSize = typeHandle.getColumnSize();
                if (columnSize > VarcharType.MAX_LENGTH || columnSize == -1) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharColumnMapping(createVarcharType(columnSize)));
        }
        return super.toPrestoType(session, connection, typeHandle);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    // Druid doesn't like table names to be qualified with catalog names in the SQL query.
    // Hence, overriding this method to pass catalog as null.
    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        String schemaName = table.getSchemaName();
        checkArgument("druid".equals(schemaName), "Only \"druid\" schema is supported");
        return new QueryBuilder(this)
                .buildSql(
                        session,
                        connection,
                        new RemoteTableName(Optional.empty(), table.getRemoteTableName().getSchemaName(), table.getRemoteTableName().getTableName()),
                        table.getGroupingSets(),
                        columns,
                        table.getConstraint(),
                        split.getAdditionalPredicate(),
                        tryApplyLimit(table.getLimit()));
    }

    /*
     * Overridden since the {@link BaseJdbcClient#getColumns(JdbcTableHandle, DatabaseMetaData)}
     * method uses character escaping that doesn't work well with Druid's Avatica handler.
     * Unfortunately, because we can't escape search characters like '_' and '%",
     * this call ends up retrieving columns for all tables that match the search
     * pattern. For ex - LIKE some_table matches somertable, somextable and some_table.
     *
     * See getColumns(ConnectorSession, JdbcTableHandle)} to look at tables are filtered.
     */
    @Override
    protected ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                null);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(DruidErrorCode.DRUID_DDL_NOT_SUPPORTED, "DDL operations are not supported in the presto-druid connector");
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(DruidErrorCode.DRUID_DDL_NOT_SUPPORTED, "DDL operations are not supported in the presto-druid connector");
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        throw new PrestoException(DruidErrorCode.DRUID_DML_NOT_SUPPORTED, "DML operations are not supported in the presto-druid connector");
    }

    @Override
    public void commitCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        throw new PrestoException(DruidErrorCode.DRUID_DDL_NOT_SUPPORTED, "DDL operations are not supported in the presto-druid connector");
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        throw new PrestoException(DruidErrorCode.DRUID_DDL_NOT_SUPPORTED, "DDL operations are not supported in the presto-druid connector");
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new PrestoException(DruidErrorCode.DRUID_DDL_NOT_SUPPORTED, "DDL operations are not supported in the presto-druid connector");
    }

    @Override
    public void dropTable(JdbcIdentity identity, JdbcTableHandle handle)
    {
        throw new PrestoException(DruidErrorCode.DRUID_DDL_NOT_SUPPORTED, "DDL operations are not supported in the presto-druid connector");
    }

    @Override
    public void rollbackCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        throw new PrestoException(DruidErrorCode.DRUID_DDL_NOT_SUPPORTED, "DDL operations are not supported in the presto-druid connector");
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        throw new PrestoException(DruidErrorCode.DRUID_DML_NOT_SUPPORTED, "DML operations are not supported in the presto-druid connector");
    }

    @Override
    public void createSchema(JdbcIdentity identity, String schemaName)
    {
        throw new PrestoException(DruidErrorCode.DRUID_DDL_NOT_SUPPORTED, "DDL operations are not supported in the presto-druid connector");
    }

    @Override
    public void dropSchema(JdbcIdentity identity, String schemaName)
    {
        throw new PrestoException(DruidErrorCode.DRUID_DDL_NOT_SUPPORTED, "DDL operations are not supported in the presto-druid connector");
    }

    private Set<AggregateFunctionRule> aggregateFunctionRules()
    {
        JdbcTypeHandle bigIntHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), 0, 0, Optional.empty(), Optional.empty());
        JdbcTypeHandle doubleHandle = new JdbcTypeHandle(Types.DOUBLE, Optional.of("double"), 0, 0, Optional.empty(), Optional.empty());
        Set<Type> numericTypes = Set.of(DOUBLE, BIGINT, REAL);
        Set<Type> numericAndTimeStampType = Set.of(DOUBLE, BIGINT, REAL, TimestampType.createTimestampType(3));
        Set<Type> allTypes = Set.of(DOUBLE, BIGINT, REAL, TimestampType.createTimestampType(3), VARCHAR);
        ImmutableSet.Builder<AggregateFunctionRule> builder = ImmutableSet.<AggregateFunctionRule>builder()
                .add(new ImplementCountAll(bigIntHandle))
                .add(new ImplementCount(bigIntHandle));

        allTypes.forEach(type ->
                builder.add(SingleInputAggregateFunction.builder()
                        .prestoName("approx_distinct")
                        .expression("approx_count_distinct(%s)")
                        .jdbcTypeHandle(bigIntHandle)
                        .outputType(BIGINT)
                        .inputTypes(type)
                        .build()));

        numericAndTimeStampType.forEach(type ->
                builder.add(
                        SingleInputAggregateFunction.builder()
                                .prestoName("max")
                                .inputTypes(type)
                                .outputType(type)
                                .build())
                        .add(SingleInputAggregateFunction.builder()
                                .prestoName("min")
                                .inputTypes(type)
                                .outputType(type)
                                .build()));

        numericTypes.forEach(type ->
                builder.add(
                        SingleInputAggregateFunction.builder()
                                .prestoName("sum")
                                .inputTypes(type)
                                .outputType(type)
                                .build())
                        .add(SingleInputAggregateFunction.builder()
                                .prestoName("avg")
                                .expression("avg(CAST(%s AS double))")
                                .inputTypes(type)
                                .outputType(DOUBLE)
                                .jdbcTypeHandle(doubleHandle)
                                .build())
                        .add(SingleInputAggregateFunction.builder()
                                .prestoName("stddev")
                                .expression("CASE WHEN COUNT(%s) > 1 THEN STDDEV(%s) ELSE NULL END")
                                .inputTypes(type)
                                .outputType(DOUBLE)
                                .jdbcTypeHandle(doubleHandle)
                                .build())
                        .add(SingleInputAggregateFunction.builder()
                                .prestoName("stddev_pop")
                                .inputTypes(type)
                                .expression("CASE WHEN COUNT(%s) = 0 THEN NULL WHEN COUNT(%s) = 1 THEN 0.0 ELSE STDDEV_POP(%s) END")
                                .outputType(DOUBLE)
                                .jdbcTypeHandle(doubleHandle)
                                .build())
                        .add(SingleInputAggregateFunction.builder()
                                .prestoName("stddev_samp")
                                .inputTypes(type)
                                .expression("CASE WHEN COUNT(%s) > 1 THEN STDDEV_SAMP(%s) ELSE NULL END")
                                .outputType(DOUBLE)
                                .jdbcTypeHandle(doubleHandle)
                                .build())
                        .add(SingleInputAggregateFunction.builder()
                                .prestoName("variance")
                                .inputTypes(type)
                                .expression("CASE WHEN COUNT(%s) > 1 THEN VARIANCE(%s) ELSE NULL END")
                                .outputType(DOUBLE)
                                .jdbcTypeHandle(doubleHandle)
                                .build())
                        .add(SingleInputAggregateFunction.builder()
                                .prestoName("var_pop")
                                .inputTypes(type)
                                .expression("CASE WHEN COUNT(%s) = 0 THEN NULL WHEN COUNT(%s) = 1 THEN 0.0 ELSE VAR_POP(%s) END")
                                .outputType(DOUBLE)
                                .jdbcTypeHandle(doubleHandle)
                                .build())
                        .add(SingleInputAggregateFunction.builder()
                                .prestoName("var_samp")
                                .inputTypes(type)
                                .expression("CASE WHEN COUNT(%s) > 1 THEN VAR_SAMP(%s) ELSE NULL END")
                                .outputType(DOUBLE)
                                .jdbcTypeHandle(doubleHandle)
                                .build()));

        return builder.build();
    }

    private static Set<FunctionRule> functionRules()
    {
        JdbcTypeHandle bigIntHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), 0, 0, Optional.empty(), Optional.empty());
        JdbcTypeHandle doubleHandle = new JdbcTypeHandle(Types.DOUBLE, Optional.of("double"), 0, 0, Optional.empty(), Optional.empty());
        JdbcTypeHandle timestampHandle = new JdbcTypeHandle(Types.TIMESTAMP, Optional.of("timestamp"), 0, 0, Optional.empty(), Optional.empty());
        JdbcTypeHandle timestampWithTimeZoneHandle = new JdbcTypeHandle(Types.TIMESTAMP_WITH_TIMEZONE, Optional.empty(), 0, 0, Optional.empty(), Optional.empty());
        JdbcTypeHandle varcharHandle = new JdbcTypeHandle(Types.VARCHAR, Optional.of("varchar"), -1, 0, Optional.empty(), Optional.empty());
        ImmutableSet.Builder<FunctionRule> builder = ImmutableSet.builder();

        builder.add(FunctionRuleDSL.builder()
                .prestoName("date_trunc")
                .expression("DATE_TRUNC(%s)")
                .jdbcTypeHandle(timestampHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("year")
                .expression("EXTRACT(YEAR from %s)")
                .jdbcTypeHandle(bigIntHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("quarter")
                .expression("EXTRACT(QUARTER from %s)")
                .jdbcTypeHandle(bigIntHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("month")
                .expression("EXTRACT(MONTH from %s)")
                .jdbcTypeHandle(bigIntHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("week")
                .expression("EXTRACT(WEEK from %s)")
                .jdbcTypeHandle(bigIntHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("day")
                .expression("EXTRACT(DAY from %s)")
                .jdbcTypeHandle(bigIntHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("day_of_week")
                .expression("EXTRACT(DOW from %s)")
                .jdbcTypeHandle(bigIntHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("day_of_year")
                .expression("EXTRACT(DOY from %s)")
                .jdbcTypeHandle(bigIntHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("hour")
                .expression("EXTRACT(HOUR from %s)")
                .jdbcTypeHandle(bigIntHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("minute")
                .expression("EXTRACT(MINUTE from %s)")
                .jdbcTypeHandle(bigIntHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("second")
                .expression("EXTRACT(SECOND from %s)")
                .jdbcTypeHandle(bigIntHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("parse_datetime")
                .expression("cast(TIME_PARSE(%s)")
                .jdbcTypeHandle(timestampWithTimeZoneHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("format_datetime")
                .expression("TIME_FORMAT(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

// literal issue
//        builder.add(FunctionRuleDSL.builder()
//                .prestoName("from_unixtime")
//                .expression("MILLIS_TO_TIMESTAMP(%s)")
//                .jdbcTypeHandle(timestampHandle)
//                .outputType(TimestampType.createTimestampType(3))
//                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("to_unixtime")
                .expression("TIMESTAMP_TO_MILLIS(%s)")
                .jdbcTypeHandle(doubleHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("date_add")
                .expression("TIMESTAMPADD(%s)")
                .jdbcTypeHandle(timestampHandle)
                .shouldQuoteStringLiterals(false)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("date_diff")
                .expression("TIMESTAMPDIFF(%s)")
                .jdbcTypeHandle(bigIntHandle)
                .shouldQuoteStringLiterals(false)
                .build());

        // STRING FUNCTIONS
        // TODO: would only work with 2 args, others will break
        builder.add(FunctionRuleDSL.builder()
                .prestoName("concat")
                .expression("CONCAT(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

        //Format does not work as the second argument is of type row
//        builder.add(FunctionRuleDSL.builder()
//                .prestoName("format")
//                .expression("STRING_FORMAT(%s)")
//                .jdbcTypeHandle(varcharHandle)
//                .outputType(VARCHAR)
//                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("length")
                .expression("LENGTH(%s)")
                .jdbcTypeHandle(bigIntHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("lower")
                .expression("LOWER(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("position")
                .expression("POSITION(%s)")
                .jdbcTypeHandle(bigIntHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("regexp_extract")
                .expression("REGEXP_EXTRACT(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

//        Fails because the function is not available in 0.16
//        builder.add(FunctionRuleDSL.builder()
//                .prestoName("regexp_like")
//                .expression("REGEXP_LIKE(%s)")
//                .jdbcTypeHandle(booleanHandle)
//                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("replace")
                .expression("REPLACE(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("regexp_replace")
                .expression("REPLACE(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

        // has 2 presto version and only one will work the other one will break
        builder.add(FunctionRuleDSL.builder()
                .prestoName("strpos")
                .expression("STRPOS(%s)")
                .jdbcTypeHandle(bigIntHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("substring")
                .expression("SUBSTRING(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("trim")
                .expression("BTRIM(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("ltrim")
                .expression("LTRIM(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("rtrim")
                .expression("RTRIM(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("upper")
                .expression("UPPER(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("reverse")
                .expression("REVERSE(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("lpad")
                .expression("LPAD(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("rpad")
                .expression("RPAD(%s)")
                .jdbcTypeHandle(varcharHandle)
                .build());

        // MATH Functions, need single input as output types depend on input types
        builder.add(FunctionRuleDSL.builder()
                .prestoName("abs")
                .expression("ABS(%s)")
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("ceiling")
                .expression("CEIL(%s)")
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("ceil")
                .expression("CEIL(%s)")
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("exp")
                .expression("EXP(%s)")
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("floor")
                .expression("FLOOR(%s)")
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("pow")
                .expression("POWER(%s,%s)")
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("power")
                .expression("POWER(%s,%s)")
                .build());

        builder.add(FunctionRuleDSL.builder()
                .prestoName("sqrt")
                .expression("SQRT(%s)")
                .build());

        return builder.build();
    }
}
