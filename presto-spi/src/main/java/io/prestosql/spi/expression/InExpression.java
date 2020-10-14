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

import io.prestosql.spi.type.BooleanType;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static java.util.Collections.emptyList;

public class InExpression
        extends ConnectorExpression
{
    public InExpression(ConnectorExpression value, ConnectorExpression valueList)
    {
        super(BooleanType.BOOLEAN);
        this.value = value;
        this.valueList = valueList;
    }

    private ConnectorExpression value;
    private ConnectorExpression valueList;

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return emptyList();
    }

    public ConnectorExpression getValue()
    {
        return value;
    }

    public ConnectorExpression getValueList()
    {
        return valueList;
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

        InExpression that = (InExpression) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(valueList, that.valueList);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, valueList);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", InExpression.class.getSimpleName() + "[", "]")
                .add("value=" + value)
                .add("valueList=" + valueList)
                .toString();
    }
}
