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
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRule;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.Verify.verifyNotNull;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.basicAggregation;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.expressionType;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.functionName;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.outputType;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.singleInput;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.variable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SingleInputAggregateFunction
        implements AggregateFunctionRule
{
    private static final Capture<Variable> INPUT = newCapture();
    private final String prestoName; // presto aggregate function name to match
    private final Optional<String> druidName; // mapping to druid name if its not same
    private final Optional<JdbcTypeHandle> jdbcTypeHandle; // type handle if its different from input column
    private Type inputType; // empty if all input types should match, or the set to match
    private Type outputType; // provide if the pattern should only match specific output type

    public SingleInputAggregateFunction(
            String prestoName,
            Optional<String> druidName,
            Optional<JdbcTypeHandle> jdbcTypeHandle,
            Type inputType,
            Type outputType)
    {
        this.prestoName = requireNonNull(prestoName, "prestoName is null");
        this.druidName = requireNonNull(druidName, "druidName is null");
        this.jdbcTypeHandle = requireNonNull(jdbcTypeHandle, "jdbcTypeHandle is null");
        this.inputType = requireNonNull(inputType, "inputType is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo(prestoName))
                .with(outputType().equalTo(outputType))
                .with(singleInput()
                        .matching(variable().with(expressionType().equalTo(inputType)).capturedAs(INPUT)));
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
        private Type inputType;
        private Type outputType;

        public Builder prestoName(String prestoName)
        {
            this.prestoName = prestoName;
            return this;
        }

        public Builder druidName(String druidName)
        {
            this.druidName = Optional.of(druidName);
            return this;
        }

        public Builder jdbcTypeHandle(JdbcTypeHandle jdbcTypeHandle)
        {
            this.jdbcTypeHandle = Optional.of(jdbcTypeHandle);
            return this;
        }

        public Builder inputTypes(Type inputType)
        {
            this.inputType = inputType;
            return this;
        }

        public Builder outputType(Type outputType)
        {
            this.outputType = outputType;
            return this;
        }

        public SingleInputAggregateFunction build()
        {
            return new SingleInputAggregateFunction(prestoName, druidName, jdbcTypeHandle, inputType, outputType);
        }
    }
}
