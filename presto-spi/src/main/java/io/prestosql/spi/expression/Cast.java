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

import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static java.util.Collections.emptyList;

public class Cast
        extends ConnectorExpression
{
    private final ConnectorExpression expression;
    private final Type type;

    public ConnectorExpression getExpression()
    {
        return expression;
    }

    @Override
    public Type getType()
    {
        return type;
    }

    public boolean isSafe()
    {
        return safe;
    }

    public boolean isTypeOnly()
    {
        return typeOnly;
    }

    private final boolean safe;
    private final boolean typeOnly;

    public Cast(ConnectorExpression expression, Type type, boolean safe, boolean typeOnly)
    {
        super(type);
        this.expression = expression;
        this.type = type;
        this.safe = safe;
        this.typeOnly = typeOnly;
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return emptyList();
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", Cast.class.getSimpleName() + "[", "]")
                .add("expression=" + expression)
                .add("type=" + type)
                .add("safe=" + safe)
                .add("typeOnly=" + typeOnly)
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

        Cast cast = (Cast) o;
        return safe == cast.safe &&
                typeOnly == cast.typeOnly &&
                Objects.equals(expression, cast.expression) &&
                Objects.equals(type, cast.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, type, safe, typeOnly);
    }
}
