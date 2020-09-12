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
import static java.util.Objects.requireNonNull;

public class FunctionCall
        extends ConnectorExpression
{
    private final String name;
    private final List<ConnectorExpression> arguments;

    public FunctionCall(String name, List<ConnectorExpression> arguments, Type type)
    {
        super(type);
        this.name = requireNonNull(name, "name can not be null");
        this.arguments = List.copyOf(requireNonNull(arguments, "arguments can not be null"));
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return emptyList();
    }

    public String getName()
    {
        return name;
    }

    public List<ConnectorExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", FunctionCall.class.getSimpleName() + "[", "]")
                .add("name='" + name + "'")
                .add("arguments=" + arguments)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        FunctionCall that = (FunctionCall) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, arguments);
    }
}
