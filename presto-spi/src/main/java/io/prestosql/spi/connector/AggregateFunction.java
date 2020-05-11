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
package io.prestosql.spi.connector;

import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class AggregateFunction
{
    private final String functionName;
    private final Type outputType;
    private final List<ConnectorExpression> inputs;
    private final List<Map.Entry<String, SortOrder>> sortOrder;
    private final boolean isDistinct;
    private final Optional<String> filter;

    public AggregateFunction(
            String aggregateFunctionName,
            Type outputType,
            List<ConnectorExpression> inputs,
            List<Map.Entry<String, SortOrder>> sortOrder,
            boolean isDistinct,
            Optional<String> filter)
    {
        this.functionName = requireNonNull(aggregateFunctionName, "name is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
        this.inputs = unmodifiableList(new ArrayList<>(requireNonNull(inputs, "inputs is null")));
        this.sortOrder = unmodifiableList(new ArrayList<>(requireNonNull(sortOrder, "sortOrder is null")));
        this.isDistinct = requireNonNull(isDistinct, "isDistinct is null");
        this.filter = filter;
    }

    public String getFunctionName()
    {
        return functionName;
    }

    public List<ConnectorExpression> getInputs()
    {
        return inputs;
    }

    public Type getOutputType()
    {
        return outputType;
    }

    public List<Map.Entry<String, SortOrder>> getSortOrder()
    {
        return sortOrder;
    }

    public boolean isDistinct()
    {
        return isDistinct;
    }

    public Optional<String> getFilter()
    {
        return filter;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", AggregateFunction.class.getSimpleName() + "[", "]")
                .add("aggregationName='" + functionName + "'")
                .add("inputs=" + inputs)
                .add("outputType=" + outputType)
                .add("sortOrder=" + sortOrder)
                .add("isDistinct=" + isDistinct)
                .add("filter=" + filter)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AggregateFunction that = (AggregateFunction) o;
        return isDistinct == that.isDistinct &&
                Objects.equals(functionName, that.functionName) &&
                Objects.equals(inputs, that.inputs) &&
                Objects.equals(outputType, that.outputType) &&
                Objects.equals(sortOrder, that.sortOrder) &&
                Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionName, inputs, outputType, sortOrder, isDistinct, filter);
    }
}
