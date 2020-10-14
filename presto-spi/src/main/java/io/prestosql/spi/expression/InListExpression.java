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
package io.prestosql.spi.expression;

import io.prestosql.spi.type.ArrayType;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static java.util.Collections.emptyList;

public class InListExpression
        extends ConnectorExpression
{
    private final List<ConnectorExpression> values;

    public InListExpression(List<ConnectorExpression> values)
    {
        super(new ArrayType(values.get(0).getType()));
        this.values = values;
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

        InListExpression that = (InListExpression) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return emptyList();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(values);
    }

    public List<ConnectorExpression> getValues()
    {
        return values;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", InListExpression.class.getSimpleName() + "[", "]")
                .add("values=" + values)
                .toString();
    }
}
