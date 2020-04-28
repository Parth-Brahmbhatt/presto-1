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
package io.prestosql.spi.function;

import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class AggregateFunction
{
    // Ideally we would want this to actually be a reference to something like AggregationImplementation or aggregationFunction registry so connectors
    // can get a one to one mapping from presto's supported aggregate function rather than relying on the name, args and output types.
    // Something to check with Martin what he thinks is the best course here, for now just exposing the fields that the connector can use to derive if
    // this is an agg function it can handle and information they need to push it down.
    private final String projectionName; // may be call it assignmentId? the purpose for this is to uniquely identify an agg but we are just using the projected expression's output symbol as the name here
    private final String aggregateFunctionName;
    private final List<ConnectorExpression> inputs;
    private final Type outputType;
    private final Optional<List<ConnectorExpression>> sortBy;
    private final Optional<Map<ConnectorExpression, SortOrder>> sortOrder;
    private final boolean isDistinct;
    // TODO It is unclear if filtering and mask from the aggregate needs to be forwarded to connectors so skipping them for now.

    public AggregateFunction(String projectionName, String name, List<ConnectorExpression> inputs, Type outputType)
    {
        this(projectionName, name, inputs, outputType, Optional.empty(), Optional.empty(), false);
    }

    public AggregateFunction(String projectionName, String aggregateFunctionName, List<ConnectorExpression> inputs, Type outputType, Optional<List<ConnectorExpression>> sortBy, Optional<Map<ConnectorExpression, SortOrder>> sortOrder, boolean isDistinct)
    {
        this.projectionName = requireNonNull(projectionName, "projectionName is null");
        this.aggregateFunctionName = requireNonNull(aggregateFunctionName, "name is null");
        this.inputs = requireNonNull(inputs, "inputs is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
        this.sortBy = requireNonNull(sortBy, "sortBy is null");
        this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
        this.isDistinct = requireNonNull(isDistinct, "isDistinct is null");
    }

    public String getProjectionName()
    {
        return projectionName;
    }

    public String getAggregateFunctionName()
    {
        return aggregateFunctionName;
    }

    public List<ConnectorExpression> getInputs()
    {
        return inputs;
    }

    public Type getOutputType()
    {
        return outputType;
    }

    public Optional<List<ConnectorExpression>> getSortBy()
    {
        return sortBy;
    }

    public Optional<Map<ConnectorExpression, SortOrder>> getSortOrder()
    {
        return sortOrder;
    }

    public boolean isDistinct()
    {
        return isDistinct;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", AggregateFunction.class.getSimpleName() + "[", "]")
                .add("projectionName='" + projectionName + "'")
                .add("aggregationName='" + aggregateFunctionName + "'")
                .add("inputs=" + inputs)
                .add("outputType=" + outputType)
                .add("sortBy=" + sortBy)
                .add("sortOrder=" + sortOrder)
                .add("isDistinct=" + isDistinct)
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
                Objects.equals(projectionName, that.projectionName) &&
                Objects.equals(aggregateFunctionName, that.aggregateFunctionName) &&
                Objects.equals(inputs, that.inputs) &&
                Objects.equals(outputType, that.outputType) &&
                Objects.equals(sortBy, that.sortBy) &&
                Objects.equals(sortOrder, that.sortOrder);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(projectionName, aggregateFunctionName, inputs, outputType, sortBy, sortOrder, isDistinct);
    }
}
