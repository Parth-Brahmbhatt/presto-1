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
import io.prestosql.matching.Property;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.FunctionCall;
import io.prestosql.spi.type.Type;

import java.util.List;

public class FunctionPatterns
{
    private FunctionPatterns() {}

    public static Pattern<FunctionCall> basicFunction()
    {
        return Pattern.typeOf(FunctionCall.class);
    }

    public static Property<FunctionCall, ?, String> functionName()
    {
        return Property.property("functionName", FunctionCall::getName);
    }

    public static Property<FunctionCall, ?, Type> outputType()
    {
        return Property.property("outputType", FunctionCall::getType);
    }

    public static Property<FunctionCall, ?, List<ConnectorExpression>> inputs()
    {
        return Property.property("inputs", FunctionCall::getArguments);
    }

//    public static Pattern<List<ConnectorExpression>> connectorExpressions()
//    {
//        return Pattern.typeOf((Class<List<ConnectorExpression>>)(Class)List.class);
//    }

    public static Property<ConnectorExpression, ?, Type> expressionType()
    {
        return Property.property("type", ConnectorExpression::getType);
    }
}
