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
package io.trino.plugin.druid.aggregate;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.AggregateFunctionPatterns;
import io.trino.plugin.jdbc.expression.AggregateFunctionRule;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.expressionType;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.singleInput;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.variable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SingleInputAggregateFunction
        implements AggregateFunctionRule
{
    private static final Capture<Variable> INPUT = newCapture();
    private final String prestoName; // presto aggregate function name to match
    private final Optional<String> expression; // the expression format
    private final Optional<JdbcTypeHandle> jdbcTypeHandle; // type handle if its different from input column
    private Type inputType; // empty if all input types should match, or the set to match
    private Type outputType; // provide if the pattern should only match specific output type

    public SingleInputAggregateFunction(
            String prestoName,
            Optional<String> expression,
            Optional<JdbcTypeHandle> jdbcTypeHandle,
            Type inputType,
            Type outputType)
    {
        this.prestoName = requireNonNull(prestoName, "prestoName is null");
        this.expression = requireNonNull(expression, "expression is null");
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
        String expressionFormat = expression.orElse(prestoName + "(%s)");
        return Optional.of(new JdbcExpression(
                expressionFormat.replaceAll("%s", columnHandle.toSqlExpression(context.getIdentifierQuote())),
                jdbcTypeHandle.orElse(columnHandle.getJdbcTypeHandle())));
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
        private Type inputType;
        private Type outputType;

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
            return new SingleInputAggregateFunction(prestoName, expression, jdbcTypeHandle, inputType, outputType);
        }
    }
}
