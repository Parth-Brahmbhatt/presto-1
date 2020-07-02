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
package io.prestosql.plugin.druid.aggregate;

import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRule;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.type.Type;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verifyNotNull;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.basicAggregation;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.expressionType;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.functionName;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.singleInput;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.variable;
import static java.lang.String.format;

/**
 * Implements a single input aggregate function
 */
public class SingleInputAggregateFunction
        implements AggregateFunctionRule
{
    private static final Capture<Variable> INPUT = newCapture();
    private final String prestoName; // presto aggregate function name to match
    private final Optional<String> druidName; // mapping to druid name if its not same
    private final Optional<JdbcTypeHandle> jdbcTypeHandle; // type handle if its different from input column
    private Set<Type> inputTypes; // empty if all input types should match, or the set to match
    private Optional<Type> outputType; // provide if the pattern should only match specific output type

    public SingleInputAggregateFunction(
            String prestoName,
            Optional<String> druidName,
            Optional<JdbcTypeHandle> jdbcTypeHandle,
            Set<Type> inputTypes,
            Optional<Type> outputType)
    {
        this.prestoName = verifyNotNull(prestoName, "prestoName is null");
        this.druidName = verifyNotNull(druidName, "druidName is null");
        this.jdbcTypeHandle = verifyNotNull(jdbcTypeHandle, "jdbcTypeHandle is null");
        this.inputTypes = verifyNotNull(inputTypes, "inputTypes is null");
        this.outputType = verifyNotNull(outputType, "outputType is null");
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        Pattern<AggregateFunction> basePattern = basicAggregation()
                .with(functionName().equalTo(prestoName));
        Pattern<AggregateFunction> pattern = outputType
                .map(type -> basePattern.with(AggregateFunctionPatterns.outputType().equalTo(type)))
                .orElse(basePattern);

        if (inputTypes.isEmpty()) {
            return pattern.with(singleInput().matching(variable().capturedAs(INPUT)));
        }
        else {
            return pattern.with(singleInput()
                    .matching(variable().with(expressionType().matching(type -> inputTypes.contains(type))).capturedAs(INPUT)));
        }
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        Variable input = captures.get(INPUT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignments().get(input.getName());
        verifyNotNull(columnHandle, "Unbound variable: %s", input);

        return Optional.of(new JdbcExpression(
                format("%s(%s)", druidName.orElse(prestoName), columnHandle.toSqlExpression(context.getIdentifierQuote())),
                jdbcTypeHandle.orElse(columnHandle.getJdbcTypeHandle())));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private String prestoName;
        private Optional<String> druidName = Optional.empty();
        private Optional<JdbcTypeHandle> jdbcTypeHandle = Optional.empty();
        private Set<Type> inputTypes = Set.of();
        private Optional<Type> outputType = Optional.empty();

        public Builder prestoName(String prestoName)
        {
            this.prestoName = prestoName;
            return this;
        }

        public Builder druidName(Optional<String> druidName)
        {
            this.druidName = druidName;
            return this;
        }

        public Builder jdbcTypeHandle(Optional<JdbcTypeHandle> jdbcTypeHandle)
        {
            this.jdbcTypeHandle = jdbcTypeHandle;
            return this;
        }

        public Builder inputTypes(Set<Type> inputTypes)
        {
            this.inputTypes = inputTypes;
            return this;
        }

        public Builder outputType(Optional<Type> outputType)
        {
            this.outputType = outputType;
            return this;
        }

        public SingleInputAggregateFunction build()
        {
            return new SingleInputAggregateFunction(prestoName, druidName, jdbcTypeHandle, inputTypes, outputType);
        }
    }
}
