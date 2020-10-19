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

import io.prestosql.matching.Pattern;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.expression.FunctionCall;
import io.prestosql.spi.type.Type;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public interface FunctionRule
{
    Pattern<FunctionCall> getPattern();

    String getPrestoName();

    Optional<String> expressionFormat();

    JdbcTypeHandle getJdbcTypeHandle(Type sourceType);

    boolean shouldQuoteStringLiterals();

    interface RewriteContext
    {
        Map<String, JdbcColumnHandle> getAssignments();

        Function<String, String> getIdentifierQuote();

        ConnectorSession getSession();
    }
}
