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

import io.prestosql.matching.Capture;
import io.prestosql.matching.Pattern;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.StandardColumnMappings;
import io.prestosql.plugin.jdbc.expression.FunctionRule;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.FunctionCall;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.plugin.jdbc.expression.FunctionPatterns.basicFunction;
import static io.prestosql.plugin.jdbc.expression.FunctionPatterns.functionName;
import static java.util.Objects.requireNonNull;

//TODO: match input types and sizes, the whole process connector expression needs to be visitor based not this if else shit.
//the shouldQuoteStringLiterals heck is ugly and the expression requiring (%s) sucks too. basically rewrite this class.
public class FunctionRuleDSL
        implements FunctionRule
{
    private static final Capture<List<ConnectorExpression>> INPUT = newCapture();
    private final String prestoName; // presto aggregate function name to match
    private final Optional<String> expressionFormat; // the expression format
    private final Optional<JdbcTypeHandle> jdbcTypeHandle; // type handle if its different from input column
    private boolean shouldQuoteStringLiterals;

    public FunctionRuleDSL(
            String prestoName,
            Optional<String> expressionFormat,
            Optional<JdbcTypeHandle> jdbcTypeHandle,
            boolean shouldQuoteStringLiterals)
    {
        this.prestoName = requireNonNull(prestoName, "prestoName is null");
        this.expressionFormat = requireNonNull(expressionFormat, "expression is null");
        this.jdbcTypeHandle = requireNonNull(jdbcTypeHandle, "jdbcTypeHandle is null");
        this.shouldQuoteStringLiterals = requireNonNull(shouldQuoteStringLiterals, "shouldQuoteStringLiterals is null");
    }

    @Override
    public Pattern<FunctionCall> getPattern()
    {
        return basicFunction()
                .with(functionName().equalTo(prestoName));
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
    public JdbcTypeHandle getJdbcTypeHandle(Type sourceType)
    {
        if (jdbcTypeHandle.isPresent()) {
            return jdbcTypeHandle.get();
        }
        final Optional<Integer> jdbcType = StandardColumnMappings.prestoTypeToJdbcType(sourceType);
        return new JdbcTypeHandle(jdbcType.get(), Optional.empty(), 0, 0, Optional.empty(), Optional.empty());
    }

    @Override
    public boolean shouldQuoteStringLiterals()
    {
        return shouldQuoteStringLiterals;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private String prestoName;
        private Optional<String> expression = Optional.empty();
        private Optional<JdbcTypeHandle> jdbcTypeHandle = Optional.empty();
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
            this.jdbcTypeHandle = Optional.of(jdbcTypeHandle);
            return this;
        }

        public Builder shouldQuoteStringLiterals(boolean shouldQuoteStringLiterals)
        {
            this.shouldQuoteStringLiterals = shouldQuoteStringLiterals;
            return this;
        }

        public FunctionRuleDSL build()
        {
            return new FunctionRuleDSL(prestoName, expression, jdbcTypeHandle, shouldQuoteStringLiterals);
        }
    }
}
