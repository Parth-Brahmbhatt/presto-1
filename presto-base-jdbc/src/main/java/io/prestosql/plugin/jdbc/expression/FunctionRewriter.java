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
package io.prestosql.plugin.jdbc.expression;

import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class FunctionRewriter
{
    private final Function<String, String> identifierQuote;
    private final Map<String, FunctionRule> rules;

    public FunctionRewriter(Function<String, String> identifierQuote, Set<FunctionRule> rules)
    {
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
        requireNonNull(rules, "rules is null");
        this.rules = rules.stream()
                .collect(Collectors.toMap(FunctionRule::getPrestoName, Function.identity()));
    }

    public FunctionRule.RewriteContext getContext(ConnectorSession session, Map<String, JdbcColumnHandle> assignments)
    {
        requireNonNull(assignments, "assignments is null");

        return new FunctionRule.RewriteContext()
        {
            @Override
            public Map<String, JdbcColumnHandle> getAssignments()
            {
                return assignments;
            }

            @Override
            public Function<String, String> getIdentifierQuote()
            {
                return identifierQuote;
            }

            @Override
            public ConnectorSession getSession()
            {
                return session;
            }
        };
    }
}
