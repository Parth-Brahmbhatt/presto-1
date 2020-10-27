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
package io.prestosql.sql.planner;

import io.prestosql.Session;
import io.prestosql.spi.expression.ArithmaticBinaryExpression;
import io.prestosql.spi.expression.CaseExpression;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.Constant;
import io.prestosql.spi.expression.FieldDereference;
import io.prestosql.spi.expression.Operator;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.TypeSignatureTranslator;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.BinaryLiteral;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CharLiteral;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.DataType;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.SimpleCaseExpression;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.WhenClause;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.metadata.ResolvedFunction.extractFunctionName;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.TINYINT;
import static io.prestosql.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.prestosql.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.prestosql.sql.tree.ArithmeticBinaryExpression.Operator.DIVIDE;
import static io.prestosql.sql.tree.ArithmeticBinaryExpression.Operator.MODULUS;
import static io.prestosql.sql.tree.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static io.prestosql.sql.tree.ArithmeticBinaryExpression.Operator.SUBTRACT;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionTranslator
{
    private ConnectorExpressionTranslator() {}

    public static Expression translate(ConnectorExpression expression, Map<String, Symbol> variableMappings, LiteralEncoder literalEncoder)
    {
        return new ConnectorToSqlExpressionTranslator(variableMappings, literalEncoder).translate(expression);
    }

    public static Optional<ConnectorExpression> translate(Session session, Expression expression, TypeAnalyzer types, TypeProvider inputTypes)
    {
        return new SqlToConnectorExpressionTranslator(types.getTypes(session, inputTypes, expression), types)
                .process(expression);
    }

    private static class ConnectorToSqlExpressionTranslator
    {
        private final Map<String, Symbol> variableMappings;
        private final LiteralEncoder literalEncoder;

        public ConnectorToSqlExpressionTranslator(Map<String, Symbol> variableMappings, LiteralEncoder literalEncoder)
        {
            this.variableMappings = requireNonNull(variableMappings, "variableMappings is null");
            this.literalEncoder = requireNonNull(literalEncoder, "literalEncoder is null");
        }

        public Expression translate(ConnectorExpression expression)
        {
            if (expression instanceof Variable) {
                return variableMappings.get(((Variable) expression).getName()).toSymbolReference();
            }

            if (expression instanceof Constant) {
                return literalEncoder.toExpression(((Constant) expression).getValue(), expression.getType());
            }

            if (expression instanceof FieldDereference) {
                FieldDereference dereference = (FieldDereference) expression;

                RowType type = (RowType) dereference.getTarget().getType();
                String name = type.getFields().get(dereference.getField()).getName().get();
                return new DereferenceExpression(translate(dereference.getTarget()), new Identifier(name));
            }

            if (expression instanceof io.prestosql.spi.expression.Cast) {
                io.prestosql.spi.expression.Cast cast = (io.prestosql.spi.expression.Cast) expression;
                final Expression castExpression = translate(cast.getExpression());
                return new Cast(castExpression, toSqlType(cast.getType()), cast.isSafe(), cast.isTypeOnly());
            }

            if (expression instanceof io.prestosql.spi.expression.FunctionCall) {
                io.prestosql.spi.expression.FunctionCall functionCall = (io.prestosql.spi.expression.FunctionCall) expression;
                final List<Expression> arguments = functionCall.getArguments().stream()
                        .map(arg -> translate(arg))
                        .collect(Collectors.toList());
                return new FunctionCall(QualifiedName.of(functionCall.getName()), arguments);
            }

            if (expression instanceof io.prestosql.spi.expression.ArithmaticBinaryExpression) {
                io.prestosql.spi.expression.ArithmaticBinaryExpression arithmaticBinaryExpression = (io.prestosql.spi.expression.ArithmaticBinaryExpression) expression;

                final Expression left = translate(arithmaticBinaryExpression.getLeft());
                final Expression right = translate(arithmaticBinaryExpression.getRight());
                final ArithmeticBinaryExpression.Operator operator = operator(arithmaticBinaryExpression.getOperator());
                return new ArithmeticBinaryExpression(operator, left, right);
            }

            throw new UnsupportedOperationException("Expression type not supported: " + expression.getClass().getName());
        }

        public ArithmeticBinaryExpression.Operator operator(io.prestosql.spi.expression.ArithmaticBinaryExpression.Operator operator)
        {
            switch (operator) {
                case ADD:
                    return ADD;
                case SUBTRACT:
                    return SUBTRACT;
                case MULTIPLY:
                    return MULTIPLY;
                case DIVIDE:
                    return DIVIDE;
                case MODULUS:
                    return MODULUS;
            }
            return null;
        }
    }

    static class SqlToConnectorExpressionTranslator
            extends AstVisitor<Optional<ConnectorExpression>, Void>
    {
        private final Map<NodeRef<Expression>, Type> types;
        private final TypeAnalyzer typeAnalyzer;

        public SqlToConnectorExpressionTranslator(Map<NodeRef<Expression>, Type> types, TypeAnalyzer typeAnalyzer)
        {
            this.types = requireNonNull(types, "types is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        }

        @Override
        protected Optional<ConnectorExpression> visitSymbolReference(SymbolReference node, Void context)
        {
            return Optional.of(new Variable(node.getName(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitStringLiteral(StringLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getSlice(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            return Optional.of(new Constant(Decimals.parse(node.getValue()).getObject(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitGenericLiteral(GenericLiteral node, Void context)
        {
            final Type type = typeOf(node);
            switch (type.getTypeId().getId()) {
                case BIGINT:
                    return Optional.of(new Constant(new BigInteger(node.getValue()), type));
                case INTEGER:
                case SMALLINT:
                case TINYINT:
                    return Optional.of(new Constant(Integer.parseInt(node.getValue()), type));
                case BOOLEAN:
                    return Optional.of(new Constant(Boolean.parseBoolean(node.getValue()), type));
                case StandardTypes.DOUBLE:
                    return Optional.of(new Constant(Double.parseDouble(node.getValue()), type));
                case StandardTypes.CHAR:
                case StandardTypes.VARCHAR:
                    return Optional.of(new Constant(utf8Slice(node.getValue()), type));
                //TODO add more type support here
//                case StandardTypes.REAL:
//                    return Optional.of(new Constant(Float.parseFloat(node.getValue()), type));
//                case DATE:
//                case StandardTypes.VARBINARY:
//                case StandardTypes.TIME_WITH_TIME_ZONE:
//                case StandardTypes.TIME:
//                case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
//                case StandardTypes.TIMESTAMP:
                case StandardTypes.DECIMAL:
                    return Optional.of(new Constant(Decimals.parse(node.getValue()), type));
                default:
                    return Optional.empty();
            }
        }

        @Override
        protected Optional<ConnectorExpression> visitCast(Cast cast, Void context)
        {
            final Optional<ConnectorExpression> expression = process(cast.getExpression());
            if (expression == null || expression.isEmpty()) {
                return Optional.empty();
            }

            final Type type = getType(cast.getType());
            if (type == null) {
                return Optional.empty();
            }

            return Optional.of(new io.prestosql.spi.expression.Cast(expression.get(), type, cast.isSafe(), cast.isTypeOnly()));
        }

        private Type getType(DataType dataType)
        {
            return typeAnalyzer.getType(TypeSignatureTranslator.toTypeSignature(dataType));
        }

        @Override
        protected Optional<ConnectorExpression> visitWhenClause(WhenClause when, Void context)
        {
            final Optional<ConnectorExpression> operand = process(when.getOperand(), context);
            final Optional<ConnectorExpression> result = process(when.getResult(), context);
            if (operand == null || result == null || operand.isEmpty() || result.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new io.prestosql.spi.expression.WhenClause(operand.get(), result.get()));
        }

        @Override
        protected Optional<ConnectorExpression> visitFunctionCall(FunctionCall functionCall, Void context)
        {
            if (functionCall.isDistinct() || functionCall.getWindow().isPresent() || functionCall.getFilter().isPresent() || functionCall.getOrderBy().isPresent() || functionCall.getNullTreatment().isPresent()) {
                return Optional.empty();
            }
            final List<ConnectorExpression> arguments = functionCall.getArguments().stream()
                    .map(expression -> process(expression, context))
                    .filter(expr -> expr != null && expr.isPresent() && expr.get() != null)
                    .map(Optional::get)
                    .collect(Collectors.toList());

            if (arguments.size() != functionCall.getArguments().size()) {
                return Optional.empty();
            }

            return Optional.of(new io.prestosql.spi.expression.FunctionCall(extractFunctionName(functionCall.getName()), arguments, typeOf(functionCall)));
        }

        @Override
        protected Optional<ConnectorExpression> visitSimpleCaseExpression(SimpleCaseExpression caseExpression, Void context)
        {
            Optional<ConnectorExpression> defaultValue = Optional.empty();
            if (caseExpression.getDefaultValue().isPresent()) {
                defaultValue = process(caseExpression.getDefaultValue().get(), context);
                if (defaultValue == null || defaultValue.isEmpty()) {
                    return Optional.empty();
                }
            }
            final Optional<ConnectorExpression> operand = process(caseExpression.getOperand(), context);
            if (operand == null || operand.isEmpty()) {
                return Optional.empty();
            }

            final List<io.prestosql.spi.expression.WhenClause> whenClauses = caseExpression.getWhenClauses().stream()
                    .map(clause -> process(clause, context))
                    .filter(expr -> expr != null && expr.isPresent() && expr.get() != null)
                    .map(e -> (io.prestosql.spi.expression.WhenClause) e.get())
                    .collect(Collectors.toList());
            if (whenClauses.size() != caseExpression.getWhenClauses().size()) {
                return Optional.empty();
            }
            return Optional.of(new CaseExpression(operand, whenClauses, defaultValue));
        }

        @Override
        protected Optional<ConnectorExpression> visitCharLiteral(CharLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getSlice(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitLongLiteral(LongLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitNullLiteral(NullLiteral node, Void context)
        {
            return Optional.of(new Constant(null, typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitSearchedCaseExpression(SearchedCaseExpression caseExpression, Void context)
        {
            Optional<ConnectorExpression> defaultValue = Optional.empty();
            if (caseExpression.getDefaultValue().isPresent()) {
                defaultValue = process(caseExpression.getDefaultValue().get(), context);
                if (defaultValue == null || defaultValue.isEmpty()) {
                    return Optional.empty();
                }
            }

            final List<io.prestosql.spi.expression.WhenClause> whenClauses = caseExpression.getWhenClauses().stream()
                    .map(clause -> process(clause, context))
                    .filter(expr -> expr != null && expr.isPresent() && expr.get() != null)
                    .map(e -> (io.prestosql.spi.expression.WhenClause) e.get())
                    .collect(Collectors.toList());
            if (whenClauses.size() != caseExpression.getWhenClauses().size()) {
                return Optional.empty();
            }
            return Optional.of(new CaseExpression(Optional.empty(), whenClauses, defaultValue));
        }

        @Override
        protected Optional<ConnectorExpression> visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            Optional<ConnectorExpression> translatedBase = process(node.getBase());
            if (translatedBase == null || translatedBase.isEmpty()) {
                return Optional.empty();
            }

            RowType rowType = (RowType) typeOf(node.getBase());
            String fieldName = node.getField().getValue();
            List<RowType.Field> fields = rowType.getFields();
            int index = -1;
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field field = fields.get(i);
                if (field.getName().isPresent() && field.getName().get().equalsIgnoreCase(fieldName)) {
                    checkArgument(index < 0, "Ambiguous field %s in type %s", field, rowType.getDisplayName());
                    index = i;
                }
            }

            checkState(index >= 0, "could not find field name: %s", node.getField());

            return Optional.of(new FieldDereference(typeOf(node), translatedBase.get(), index));
        }

        @Override
        protected Optional<ConnectorExpression> visitComparisonExpression(ComparisonExpression comparisonExpression, Void context)
        {
            final Optional<ConnectorExpression> leftExpression = process(comparisonExpression.getLeft(), context);
            if (leftExpression == null || leftExpression.isEmpty()) {
                return Optional.empty();
            }
            final Optional<ConnectorExpression> rightExpression = process(comparisonExpression.getRight(), context);
            if (rightExpression == null || rightExpression.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(
                    new io.prestosql.spi.expression.ComparisonExpression(
                            operator(comparisonExpression.getOperator()),
                            leftExpression.get(),
                            rightExpression.get()));
        }

        @Override
        protected Optional<ConnectorExpression> visitInPredicate(InPredicate node, Void context)
        {
            final Optional<ConnectorExpression> value = process(node.getValue(), context);
            if (value == null || value.isEmpty()) {
                return Optional.empty();
            }
            final Optional<ConnectorExpression> valueList = process(node.getValueList(), context);
            if (valueList == null || valueList.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(
                    new io.prestosql.spi.expression.InExpression(
                            value.get(),
                            valueList.get()));
        }

        @Override
        protected Optional<ConnectorExpression> visitInListExpression(InListExpression node, Void context)
        {
            final List<ConnectorExpression> values = node.getValues().stream()
                    .map(value -> process(value, context))
                    .filter(x -> x != null && x.isPresent() && x.get() != null)
                    .map(Optional::get)
                    .collect(Collectors.toList());

            if (values.size() != node.getValues().size()) {
                return Optional.empty();
            }

            return Optional.of(new io.prestosql.spi.expression.InListExpression(values));
        }

        @Override
        protected Optional<ConnectorExpression> visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            final ArithmaticBinaryExpression.Operator operator = operator(node.getOperator());
            final Optional<ConnectorExpression> left = process(node.getLeft(), context);
            final Optional<ConnectorExpression> right = process(node.getRight(), context);
            if (operator == null || left == null || right == null || left.isEmpty() || right.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new ArithmaticBinaryExpression(operator, left.get(), right.get()));
        }

        private Type typeOf(Expression node)
        {
            return types.get(NodeRef.of(node));
        }

        private Operator operator(ComparisonExpression.Operator operator)
        {
            switch (operator) {
                case EQUAL:
                    return Operator.EQUAL;
                case NOT_EQUAL:
                    return Operator.NOT_EQUAL;
                case LESS_THAN:
                    return Operator.LESS_THAN;
                case LESS_THAN_OR_EQUAL:
                    return Operator.LESS_THAN_OR_EQUAL;
                case GREATER_THAN:
                    return Operator.GREATER_THAN;
                case GREATER_THAN_OR_EQUAL:
                    return Operator.GREATER_THAN_OR_EQUAL;
                case IS_DISTINCT_FROM:
                    return Operator.IS_DISTINCT_FROM;
            }
            return null;
        }

        private ArithmaticBinaryExpression.Operator operator(io.prestosql.sql.tree.ArithmeticBinaryExpression.Operator operator)
        {
            switch (operator) {
                case ADD:
                    return ArithmaticBinaryExpression.Operator.ADD;
                case SUBTRACT:
                    return ArithmaticBinaryExpression.Operator.SUBTRACT;
                case MULTIPLY:
                    return ArithmaticBinaryExpression.Operator.MULTIPLY;
                case DIVIDE:
                    return ArithmaticBinaryExpression.Operator.DIVIDE;
                case MODULUS:
                    return ArithmaticBinaryExpression.Operator.MODULUS;
            }
            return null;
        }
    }
}
