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
package io.prestosql.iceberg;

import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.expressions.And;
import com.netflix.iceberg.expressions.BoundReference;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.NamedReference;
import com.netflix.iceberg.expressions.Not;
import com.netflix.iceberg.expressions.Or;
import com.netflix.iceberg.expressions.Predicate;
import com.netflix.iceberg.expressions.Reference;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.iceberg.type.TypeConveter;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarcharType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.netflix.iceberg.expressions.Expressions.and;
import static com.netflix.iceberg.expressions.Expressions.equal;
import static com.netflix.iceberg.expressions.Expressions.greaterThan;
import static com.netflix.iceberg.expressions.Expressions.greaterThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.isNull;
import static com.netflix.iceberg.expressions.Expressions.lessThan;
import static com.netflix.iceberg.expressions.Expressions.lessThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.or;
import static io.prestosql.spi.predicate.Domain.create;
import static io.prestosql.spi.predicate.ValueSet.ofRanges;
import static io.prestosql.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.prestosql.spi.type.Decimals.encodeScaledValue;

// TODO Wish there was a way to actually get condition expressions instead of dealing with Domain
public class ExpressionConverterNew
{
    private ExpressionConverterNew()
    {}

    public static Expression toExpression(TupleDomain<HiveColumnHandle> tupleDomain, ConnectorSession session)
    {
        if (tupleDomain.isAll()) {
            return Expressions.alwaysTrue();
        }
        else if (tupleDomain.isNone()) {
            return Expressions.alwaysFalse();
        }
        else {
            final Map<HiveColumnHandle, Domain> tDomainMap = tupleDomain.getDomains().get();
            Expression expression = Expressions.alwaysTrue();
            for (Map.Entry<HiveColumnHandle, Domain> tDomainEntry : tDomainMap.entrySet()) {
                final HiveColumnHandle key = tDomainEntry.getKey();
                final Domain domain = tDomainEntry.getValue();
                expression = Expressions.and(expression, toExpression(key, domain, session));
            }
            return expression;
        }
    }

    private static Expression toExpression(HiveColumnHandle column, Domain domain, ConnectorSession session)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? Expressions.isNull(column.getName()) : Expressions.alwaysFalse();
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? Expressions.alwaysTrue() : Expressions.notNull(column.getName());
        }

        List<Object> singleValues = new ArrayList<>();
        Expression expr = Expressions.alwaysFalse();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(handleSlices(domain.getType(), range.getLow().getValue()));
            }
            else {
                Expression expression = Expressions.alwaysTrue();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            expression = and(expression, greaterThan(column.getName(), range.getLow().getValue()));
                            break;
                        case EXACTLY:
                            expression = and(expression, greaterThanOrEqual(column.getName(), range.getLow().getValue()));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }

                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        case EXACTLY:
                            expression = and(expression, lessThanOrEqual(column.getName(), range.getHigh().getValue()));
                            break;
                        case BELOW:
                            expression = and(expression, lessThan(column.getName(), range.getHigh().getValue()));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                expr = or(expr, expression);
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            expr = or(expr, equal(column.getName(), getOnlyElement(singleValues)));
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                expr = or(expr, equal(column.getName(), value));
            }
        }

        if (domain.isNullAllowed()) {
            expr = or(expr, isNull(column.getName()));
        }

        return expr;
    }

    private static Object getValue(Type type, Object value)
    {
        if (type instanceof TimestampType || type instanceof TimestampWithTimeZoneType || type instanceof TimeType || type instanceof TimeWithTimeZoneType) {
            // iceberg does not support zone preservation.
            return TimeUnit.NANOSECONDS.toMillis((Long) value);
        }
        else if (type instanceof VarcharType) {
            return Slices.utf8Slice(value.toString());
        }
        else if (type instanceof DecimalType) {
            if (((DecimalType) type).getPrecision() <= MAX_SHORT_PRECISION) {
                return encodeScaledValue(new BigDecimal(String.valueOf(value)));
            }
            else {
                return encodeScaledValue(new BigDecimal(String.valueOf(value)));
            }
        }
        else if (type.getJavaType() == Long.class && value instanceof Integer) {
            // TODO this is not good, The range creation internally uses a Util class that validates if the value type matches with type's java class
            // representation. But does not coerce for things like IntegerType which is represented with java `long` but the value it self could be int.
            return Long.valueOf((Integer) value);
        }
        else if (type instanceof RealType && value instanceof Float) {
            return Long.valueOf(Float.floatToIntBits((Float) value));
        }
        return value;
    }

    public static TupleDomain<HiveColumnHandle> fromIceberg(Expression expression, Map<String, HiveColumnHandle> hiveColumnHandleMap, Schema schema, TypeManager typeManager)
    {
        // this assumes that null allowed would be a separate domainTuple
        final boolean nullAllowed = false;
        switch (expression.op()) {
            case TRUE:
                return TupleDomain.all();
            case FALSE:
                return TupleDomain.none();
            case AND:
                final And and = (And) expression;
                TupleDomain<HiveColumnHandle> left = fromIceberg(and.left(), hiveColumnHandleMap, schema, typeManager);
                TupleDomain<HiveColumnHandle> right = fromIceberg(and.right(), hiveColumnHandleMap, schema, typeManager);
                return left.intersect(right);
            case OR:
                final Or or = (Or) expression;
                left = fromIceberg(or.left(), hiveColumnHandleMap, schema, typeManager);
                right = fromIceberg(or.right(), hiveColumnHandleMap, schema, typeManager);
                return TupleDomain.columnWiseUnion(left, right);
            case NOT:
                return fromIceberg(((Not) expression).child(), hiveColumnHandleMap, schema, typeManager);
            case IS_NULL:
                return fromIceberg(expression, hiveColumnHandleMap, schema, typeManager, Domain::onlyNull);
            case NOT_NULL:
                return fromIceberg(expression, hiveColumnHandleMap, schema, typeManager, Domain::notNull);
            case EQ:
                return fromIceberg(expression, schema, typeManager, hiveColumnHandleMap, nullAllowed, Range::equal);
            case NOT_EQ:
                Predicate<?, Reference> predicate = (Predicate) expression;
                int fieldId = getFieldIdByName(predicate.ref(), schema);
                Type type = TypeConveter.convert(schema.findType(fieldId), typeManager);
                ValueSet values = ofRanges(Range.lessThan(type, getValue(type, predicate.literal().value())), Range.greaterThan(type, getValue(type, predicate.literal().value()))).complement();
                return TupleDomain.withColumnDomains(ImmutableMap.of(hiveColumnHandleMap.get(schema.findColumnName(fieldId)), create(values, nullAllowed)));
            case LT:
                return fromIceberg(expression, schema, typeManager, hiveColumnHandleMap, nullAllowed, Range::lessThan);
            case GT:
                return fromIceberg(expression, schema, typeManager, hiveColumnHandleMap, nullAllowed, Range::greaterThan);
            case LT_EQ:
                return fromIceberg(expression, schema, typeManager, hiveColumnHandleMap, nullAllowed, Range::lessThanOrEqual);
            case GT_EQ:
                return fromIceberg(expression, schema, typeManager, hiveColumnHandleMap, nullAllowed, Range::greaterThanOrEqual);
            case IN: // implemented through chaining EQ operation with OR
            case NOT_IN: // implemented through chaining NOT_EQ operation with OR
                throw new RuntimeException("IN and NOT_IN expression are not expected as those expressions are represented by chaining EQ/NOT_EQ with OR.");
            default:
                throw new RuntimeException("Can't handle operation " + expression.op());
        }
    }

    private static TupleDomain fromIceberg(Expression expression,
            Schema schema,
            TypeManager typeManager,
            Map<String, HiveColumnHandle> hiveColumnHandleMap,
            boolean nullAllowed, BiFunction<Type, Object, Range> valueExtractor)
    {
        Predicate<?, Reference> predicate = (Predicate) expression;
        int fieldId = getFieldIdByName(predicate.ref(), schema);
        Type type = TypeConveter.convert(schema.findType(fieldId), typeManager);
        ValueSet values = ofRanges(valueExtractor.apply(type, getValue(type, predicate.literal().value())));
        return TupleDomain.withColumnDomains(ImmutableMap.of(hiveColumnHandleMap.get(schema.findColumnName(fieldId)), create(values, nullAllowed)));
    }

    private static TupleDomain<HiveColumnHandle> fromIceberg(Expression expression,
            Map<String, HiveColumnHandle> hiveColumnHandleMap,
            Schema schema,
            TypeManager typeManager,
            Function<Type, Domain> domainExtractor)
    {
        Predicate<?, Reference> predicate = (Predicate) expression;
        int fieldId = getFieldIdByName(predicate.ref(), schema);
        Type type = TypeConveter.convert(schema.findType(fieldId), typeManager);
        return TupleDomain.withColumnDomains(ImmutableMap.of(hiveColumnHandleMap.get(schema.findColumnName(fieldId)), domainExtractor.apply(type)));
    }

    private static <R extends Reference> int getFieldIdByName(R reference, Schema schema)
    {
        if (reference instanceof BoundReference) {
            return ((BoundReference) reference).fieldId();
        }
        else if (reference instanceof NamedReference) {
            return schema.findField(((NamedReference) reference).name()).fieldId();
        }
        else {
            throw new UnsupportedOperationException("Iceberg Predicates should only be of BoundReference or NamedReference type but got " + reference.getClass().getSimpleName());
        }
    }

    private static final Object handleSlices(Type type, Object value)
    {
        if (type.getJavaType() == Slice.class) {
            Slice slice = (Slice) value;
            return type.getClass().equals(VarcharType.class) ? slice.toStringUtf8() : slice.getBytes();
        }
        return value;
    }
}
