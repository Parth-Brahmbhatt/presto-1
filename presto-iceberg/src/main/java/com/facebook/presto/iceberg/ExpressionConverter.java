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
package com.facebook.presto.iceberg;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.EquatableValueSet;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.DateTimeEncoding;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.predicate.Marker.Bound.ABOVE;
import static com.facebook.presto.spi.predicate.Marker.Bound.BELOW;
import static com.facebook.presto.spi.predicate.Marker.Bound.EXACTLY;
import static com.facebook.presto.spi.type.StandardTypes.TIME;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.StandardTypes.VARBINARY;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.netflix.iceberg.expressions.Expressions.and;
import static com.netflix.iceberg.expressions.Expressions.equal;
import static com.netflix.iceberg.expressions.Expressions.greaterThan;
import static com.netflix.iceberg.expressions.Expressions.greaterThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.lessThan;
import static com.netflix.iceberg.expressions.Expressions.lessThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.or;

// TODO Wish there was a way to actually get condition expressions instead of dealing with Domain
public class ExpressionConverter
{
    private ExpressionConverter()
    {}

    public static Expression toIceberg(TupleDomain<HiveColumnHandle> tupleDomain, ConnectorSession session)
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
                expression = Expressions.and(expression, toIceberg(key, domain, session));
            }
            return expression;
        }
    }

    private static Expression toIceberg(HiveColumnHandle column, Domain domain, ConnectorSession session)
    {
        String columnName = column.getName();
        if (domain.isAll()) {
            return Expressions.alwaysTrue();
        }
        else if (domain.isNone()) {
            return Expressions.alwaysFalse();
        }
        else if (domain.isOnlyNull()) {
            return Expressions.isNull(columnName);
        }
        else {
            final ValueSet domainValues = domain.getValues();
            Expression expression = null;
            if (domain.isNullAllowed()) {
                expression = Expressions.isNull(columnName);
            }

            if (domainValues instanceof EquatableValueSet) {
                expression = (expression == null ? Expressions.alwaysFalse() : expression);
                if (((EquatableValueSet) domainValues).isWhiteList()) {
                    // if whitelist is true than this is a case of "in", otherwise this is a case of "not in".
                    return or(expression, equal(columnName, ((EquatableValueSet) domainValues).getValues()));
                }
                else {
                    return or(expression, Expressions.notEqual(columnName, ((EquatableValueSet) domainValues).getValues()));
                }
            }
            else {
                if (domainValues instanceof SortedRangeSet) {
                    final List<Range> orderedRanges = ((SortedRangeSet) domainValues).getOrderedRanges();
                    expression = (expression == null ? Expressions.alwaysFalse() : expression);
                    for (Range range : orderedRanges) {
                        final Marker low = range.getLow();
                        final Marker high = range.getHigh();
                        final Marker.Bound lowBound = low.getBound();
                        final Marker.Bound highBound = high.getBound();

                        // case col <> 'val' is represented as (col < 'val' or col > 'val')
                        if (lowBound.equals(EXACTLY) && highBound.equals(EXACTLY)) {
                            // case ==
                            if (getValue(column, low, session).equals(getValue(column, high, session))) {
                                expression = or(expression, equal(columnName, getValue(column, low, session)));
                            }
                            else { // case between
                                final Expression between = and(greaterThanOrEqual(columnName, getValue(column, low, session)), lessThanOrEqual(columnName, getValue(column, high, session)));
                                expression = or(expression, between);
                            }
                        }
                        else {
                            if (lowBound.equals(EXACTLY) && low.getValueBlock().isPresent()) {
                                // case >=
                                expression = or(expression, greaterThanOrEqual(columnName, getValue(column, low, session)));
                            }
                            else if (lowBound.equals(ABOVE) && low.getValueBlock().isPresent()) {
                                // case >
                                expression = or(expression, greaterThan(columnName, getValue(column, low, session)));
                            }

                            if (highBound.equals(EXACTLY) && high.getValueBlock().isPresent()) {
                                // case <=
                                expression = or(expression, lessThanOrEqual(columnName, getValue(column, high, session)));
                            }
                            else if (highBound.equals(BELOW) && high.getValueBlock().isPresent()) {
                                // case <
                                expression = or(expression, lessThan(columnName, getValue(column, high, session)));
                            }
                        }
                    }
                }
                else {
                    throw new IllegalStateException("Did not expect a domain value set other than SortedRangeSet and EquatableValueSet but got " + domainValues.getClass().getSimpleName());
                }
            }
            return expression;
        }
    }

    private static Object getValue(HiveColumnHandle columnHandle, Marker marker, ConnectorSession session)
    {
        final String base = columnHandle.getTypeSignature().getBase();
        if (base.equals(TIMESTAMP_WITH_TIME_ZONE) || base.equals(TIME_WITH_TIME_ZONE)) {
            return TimeUnit.MILLISECONDS.toMicros(DateTimeEncoding.unpackMillisUtc((Long) marker.getValue()));
        }
        else if (base.equals(TIME) || base.equals(TIMESTAMP)) {
            return TimeUnit.MILLISECONDS.toMicros((Long) marker.getValue());
        }
        else if (base.equals(VARCHAR)) {
            return marker.getPrintableValue(session);
        }
        else if (base.equals(VARBINARY)) {
            return ((Slice) marker.getValue()).getBytes();
        }
        return marker.getValue();
    }
}
