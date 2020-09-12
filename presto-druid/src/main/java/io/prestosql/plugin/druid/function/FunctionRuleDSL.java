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
package io.prestosql.plugin.druid.function;

import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.expression.FunctionRule;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.Constant;
import io.prestosql.spi.expression.FunctionCall;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.type.Type;

import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.plugin.jdbc.expression.FunctionPatterns.basicFunction;
import static io.prestosql.plugin.jdbc.expression.FunctionPatterns.functionName;
import static io.prestosql.plugin.jdbc.expression.FunctionPatterns.outputType;
import static java.util.Objects.requireNonNull;

//TODO: match input types and sizes, the whole process connector expression needs to be visitor based not this if else shit.
//the shouldQuoteStringLiterals heck is ugly and the expression requiring (%s) sucks too. basically rewrite this class.
public class FunctionRuleDSL
        implements FunctionRule
{
    private static final Capture<List<ConnectorExpression>> INPUT = newCapture();
    private final String prestoName; // presto aggregate function name to match
    private final Optional<String> expressionFormat; // the expression format
    private final JdbcTypeHandle jdbcTypeHandle; // type handle if its different from input column
    private Type outputType; // provide if the pattern should only match specific output type
    private boolean shouldQuoteStringLiterals;

    public FunctionRuleDSL(
            String prestoName,
            Optional<String> expressionFormat,
            JdbcTypeHandle jdbcTypeHandle,
            Type outputType,
            boolean shouldQuoteStringLiterals)
    {
        this.prestoName = requireNonNull(prestoName, "prestoName is null");
        this.expressionFormat = requireNonNull(expressionFormat, "expression is null");
        this.jdbcTypeHandle = requireNonNull(jdbcTypeHandle, "jdbcTypeHandle is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
        this.shouldQuoteStringLiterals = requireNonNull(shouldQuoteStringLiterals, "shouldQuoteStringLiterals is null");
    }

    @Override
    public Pattern<FunctionCall> getPattern()
    {
        return basicFunction()
                .with(functionName().equalTo(prestoName))
                .with(outputType().equalTo(outputType));
    }

    @Override
    public Optional<JdbcExpression> rewrite(FunctionCall function, Captures captures, RewriteContext context)
    {
        return processConnectorExpression(function, context, shouldQuoteStringLiterals);
    }

    @Override
    public String getPrestoName()
    {
        return prestoName;
    }

    @Override
    public Optional<String> expressionFormat()
    {
        return expressionFormat;
    }

    @Override
    public JdbcTypeHandle getJdbcTypeHandle()
    {
        return jdbcTypeHandle;
    }

    @Override
    public boolean shouldQuoteStringLiterals()
    {
        return shouldQuoteStringLiterals;
    }

    private Optional<JdbcExpression> processConnectorExpression(FunctionCall functionCall, RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        final Optional<FunctionRule> rule = context.getRule(functionCall.getName());
        if (rule.isPresent()) {
            final Optional<String> expressionFormat = rule.get().expressionFormat();
            final JdbcTypeHandle jdbcTypeHandle = rule.get().getJdbcTypeHandle();
            String expression = expressionFormat.orElse(prestoName + "(%s)");
            final List<String> arguments = functionCall.getArguments().stream()
                    .map(arg -> processConnectorExpression(arg, context, rule.get().shouldQuoteStringLiterals()))
                    .filter(Optional::isPresent)
                    .map(arg -> arg.get().getExpression())
                    .collect(Collectors.toList());

            if (arguments.size() != functionCall.getArguments().size()) {
                return Optional.empty();
            }
            expression = String.format(expression, Joiner.on(",").join(arguments));

            return Optional.of(new JdbcExpression(expression, jdbcTypeHandle));
        }
        return Optional.empty();
    }

    private Optional<JdbcExpression> processConnectorExpression(ConnectorExpression connectorExpression, RewriteContext context, boolean shouldQuoteStringLiterals)
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
        throw new UnsupportedOperationException("can't handle " + connectorExpression);
    }

    private Optional<JdbcExpression> processConnectorExpression(Constant constant, RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        String value = constant.getValue().toString();
        if (constant.getValue() instanceof Slice) {
            if (shouldQuoteStringLiterals) {
                value = String.format("'%s'", ((Slice) constant.getValue()).toStringUtf8());
            }
            else {
                value = ((Slice) constant.getValue()).toStringUtf8();
            }
        }
        return Optional.of(new JdbcExpression(
                value,
                new JdbcTypeHandle(Types.VARCHAR, Optional.empty(), 0, 0, Optional.empty(), Optional.empty()))
        );
    }

    private Optional<JdbcExpression> processConnectorExpression(Variable variable, RewriteContext context, boolean shouldQuoteStringLiterals)
    {
        return Optional.of(new JdbcExpression(
                context.getAssignments().get(variable.getName()).toSqlExpression(name -> context.getIdentifierQuote().apply(name)),
                context.getAssignments().get(variable.getName()).getJdbcTypeHandle()
        ));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private String prestoName;
        private Optional<String> expression = Optional.empty();
        private JdbcTypeHandle jdbcTypeHandle;
        private Type outputType;
        private boolean shouldQuoteStringLiterals = true;

        public Builder prestoName(String prestoName)
        {
            this.prestoName = prestoName;
            return this;
        }

        public Builder expression(String expression)
        {
            this.expression = Optional.of(expression);
            return this;
        }

        public Builder jdbcTypeHandle(JdbcTypeHandle jdbcTypeHandle)
        {
            this.jdbcTypeHandle = jdbcTypeHandle;
            return this;
        }

        public Builder outputType(Type outputType)
        {
            this.outputType = outputType;
            return this;
        }

        public Builder shouldQuoteStringLiterals(boolean shouldQuoteStringLiterals)
        {
            this.shouldQuoteStringLiterals = shouldQuoteStringLiterals;
            return this;
        }

        public FunctionRuleDSL build()
        {
            return new FunctionRuleDSL(prestoName, expression, jdbcTypeHandle, outputType, shouldQuoteStringLiterals);
        }
    }
}
