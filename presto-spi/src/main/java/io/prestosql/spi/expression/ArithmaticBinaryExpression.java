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

public class ArithmaticBinaryExpression
        extends ConnectorExpression
{
    private final Operator operator;
    private final ConnectorExpression left;
    private final ConnectorExpression right;

    public ArithmaticBinaryExpression(Operator operator, ConnectorExpression left, ConnectorExpression right)
    {
        super(BooleanType.BOOLEAN);
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", ArithmaticBinaryExpression.class.getSimpleName() + "[", "]")
                .add("operator=" + operator)
                .add("left=" + left)
                .add("right=" + right)
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
        ArithmaticBinaryExpression that = (ArithmaticBinaryExpression) o;
        return operator == that.operator &&
                Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, left, right);
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return List.of();
    }

    public enum Operator
    {
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        MODULUS("%");
        private final String value;

        Operator(String value)
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }
    }

    public Operator getOperator()
    {
        return operator;
    }

    public ConnectorExpression getLeft()
    {
        return left;
    }

    public ConnectorExpression getRight()
    {
        return right;
    }
}
