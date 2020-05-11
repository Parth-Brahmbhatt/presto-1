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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.AggregationPushdownResult;
import io.prestosql.spi.connector.Assignment;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SortOrder;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.ConnectorExpressionTranslator;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AggregationNode.GroupingSetDescriptor;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;

public class PushAggregationIntoTableScan
        implements Rule<AggregationNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(tableScan().capturedAs(TABLE_SCAN)));

    private final Metadata metadata;

    public PushAggregationIntoTableScan(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);
        Map<String, ColumnHandle> assignments = tableScan.getAssignments()
                .entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Entry::getValue));

        List<Entry<Symbol, AggregationNode.Aggregation>> aggregations = node.getAggregations()
                .entrySet()
                .stream()
                .collect(toImmutableList());

        List<AggregateFunction> aggregateFunctions = toAggregateFunction(aggregations
                .stream()
                .map(Entry::getValue)
                .collect(toImmutableList()));
        List<Symbol> aggregationOutputSymbols = aggregations.stream()
                .map(Entry::getKey)
                .collect(toImmutableList());

        if (aggregateFunctions.isEmpty()) {
            return Result.empty();
        }

        GroupingSetDescriptor groupingSets = node.getGroupingSets();

        if (groupingSets.getGroupingSetCount() > 1) {
            return Result.empty();
        }

        List<ColumnHandle> groupByColumns = groupingSets.getGroupingKeys().stream()
                .map(groupByColumn -> assignments.get(groupByColumn.getName()))
                .collect(toImmutableList());

        Optional<AggregationPushdownResult<TableHandle>> aggregationPushdownResult = metadata.applyAggregation(
                context.getSession(),
                tableScan.getTable(),
                aggregateFunctions,
                assignments,
                groupByColumns.isEmpty() ? ImmutableList.of() : ImmutableList.of(groupByColumns));

        if (!aggregationPushdownResult.isPresent()) {
            return Result.empty();
        }

        AggregationPushdownResult<TableHandle> result = aggregationPushdownResult.get();

        TableHandle handle = result.getHandle();

        // The new scan outputs and assignments are union of existing and the ones returned by connector pushdown.
        ImmutableList.Builder<Symbol> newScanOutputs = new ImmutableList.Builder<>();
        newScanOutputs.addAll(tableScan.getOutputSymbols());

        ImmutableMap.Builder<Symbol, ColumnHandle> newScanAssignments = new ImmutableMap.Builder<>();
        newScanAssignments.putAll(tableScan.getAssignments());

        Map<String, Symbol> variableMappings = new HashMap<>();

        for (Assignment assignment : result.getAssignments()) {
            Symbol symbol = context.getSymbolAllocator().newSymbol(assignment.getVariable(), assignment.getType());

            newScanOutputs.add(symbol);
            newScanAssignments.put(symbol, assignment.getColumn());
            variableMappings.put(assignment.getVariable(), symbol);
        }

        List<Expression> newPartialProjections = result.getProjections().stream()
                .map(expression -> ConnectorExpressionTranslator.translate(expression, variableMappings, new LiteralEncoder(metadata)))
                .collect(toImmutableList());

        Assignments.Builder assignmentBuilder = Assignments.builder();
        IntStream.range(0, aggregationOutputSymbols.size())
                .forEach(index -> assignmentBuilder.put(aggregationOutputSymbols.get(index), newPartialProjections.get(index)));

        // projections assignmentBuilder should have both agg and group by so we add all the group bys as symbol references
        groupingSets.getGroupingKeys().stream()
                .forEach(groupBySymbol -> assignmentBuilder.put(groupBySymbol, groupBySymbol.toSymbolReference()));

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        TableScanNode.newInstance(
                                context.getIdAllocator().getNextId(),
                                handle,
                                newScanOutputs.build(),
                                newScanAssignments.build()),
                        assignmentBuilder.build()));
    }

    private List<AggregateFunction> toAggregateFunction(List<AggregationNode.Aggregation> aggregations)
    {
        ImmutableList.Builder<AggregateFunction> aggregateFunctionBuilder = ImmutableList.builder();
        for (AggregationNode.Aggregation aggregation : aggregations) {
            Signature signature = aggregation.getResolvedFunction().getSignature();
            Type returnType = metadata.getType(signature.getReturnType());
            String name = signature.getName();
            List<Expression> arguments = aggregation.getArguments();
            List<Symbol> symbols = arguments.stream()
                    .filter(expression -> expression instanceof SymbolReference)
                    .map(SymbolsExtractor::extractUnique)
                    .flatMap(Collection::stream)
                    .collect(toImmutableList());

            if (arguments.size() != symbols.size()) {
                return ImmutableList.of();
            }

            ImmutableList.Builder<ConnectorExpression> args = new ImmutableList.Builder<>();
            for (int i = 0; i < arguments.size(); i++) {
                args.add(new Variable(symbols.get(i).getName(), metadata.getType(signature.getArgumentTypes().get(i))));
            }

            Optional<OrderingScheme> orderingScheme = aggregation.getOrderingScheme();
            Optional<List<Map.Entry<String, SortOrder>>> sortBy = orderingScheme.map(orderings ->
                    orderings.getOrderBy().stream()
                            .map(orderBy -> new AbstractMap.SimpleEntry<String, SortOrder>(
                                    orderBy.getName(),
                                    SortOrder.valueOf(orderings.getOrderings().get(orderBy).name())))
                            .collect(toImmutableList()));

            aggregateFunctionBuilder.add(new AggregateFunction(
                    name,
                    returnType,
                    args.build(),
                    sortBy.orElse(ImmutableList.of()),
                    aggregation.isDistinct(),
                    aggregation.getFilter().map(Symbol::getName)));
        }
        return aggregateFunctionBuilder.build();
    }
}
