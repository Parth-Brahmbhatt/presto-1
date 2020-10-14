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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

public class CaseExpression
        extends ConnectorExpression
{
    private final Optional<ConnectorExpression> operand;
    private final List<WhenClause> whenClauses;
    private final Optional<ConnectorExpression> defaultValue;

    public CaseExpression(Optional<ConnectorExpression> operand, List<WhenClause> whenClauses, Optional<ConnectorExpression> defaultValue)
    {
        super(defaultValue.orElse(whenClauses.get(0)).getType());
        this.operand = operand;
        this.whenClauses = whenClauses;
        this.defaultValue = defaultValue;
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return Collections.emptyList();
    }

    public Optional<ConnectorExpression> getOperand()
    {
        return operand;
    }

    public List<WhenClause> getWhenClauses()
    {
        return whenClauses;
    }

    public Optional<ConnectorExpression> getDefaultValue()
    {
        return defaultValue;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", CaseExpression.class.getSimpleName() + "[", "]")
                .add("operand=" + operand)
                .add("whenClauses=" + whenClauses)
                .add("defaultValue=" + defaultValue)
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

        CaseExpression that = (CaseExpression) o;
        return Objects.equals(operand, that.operand) &&
                Objects.equals(whenClauses, that.whenClauses) &&
                Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operand, whenClauses, defaultValue);
    }
}
