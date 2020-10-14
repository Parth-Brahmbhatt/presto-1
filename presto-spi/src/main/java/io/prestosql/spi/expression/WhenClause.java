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

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static java.util.Collections.emptyList;

public class WhenClause
        extends ConnectorExpression
{
    private final ConnectorExpression operand;
    private final ConnectorExpression result;

    public WhenClause(ConnectorExpression operand, ConnectorExpression result)
    {
        super(result.getType());
        this.operand = operand;
        this.result = result;
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return emptyList();
    }

    public ConnectorExpression getOperand()
    {
        return operand;
    }

    public ConnectorExpression getResult()
    {
        return result;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", WhenClause.class.getSimpleName() + "[", "]")
                .add("operand=" + operand)
                .add("result=" + result)
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

        WhenClause that = (WhenClause) o;
        return Objects.equals(operand, that.operand) &&
                Objects.equals(result, that.result);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operand, result);
    }
}
