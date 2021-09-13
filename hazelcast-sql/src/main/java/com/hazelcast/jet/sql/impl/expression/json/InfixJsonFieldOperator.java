/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.expression.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.BiExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;
import java.util.Map;

public class InfixJsonFieldOperator extends BiExpression<Object> implements IdentifiedDataSerializable {
    private static final ObjectMapper SERIALIZER = new ObjectMapper();

    public InfixJsonFieldOperator() { }

    private InfixJsonFieldOperator(Expression<?> operand1, Expression<?> operand2) {
        super(operand1, operand2);
    }

    public static InfixJsonFieldOperator create(Expression<?> operand1, Expression<?> operand2) {
        return new InfixJsonFieldOperator(operand1, operand2);
    }

    @Override
    public Object eval(final Row row, final ExpressionEvalContext context) {
        final String json = operand1.eval(row, context).toString();
        final String path = extractPath(operand2.eval(row, context));

        final Object result = JsonPathUtil.read(json, path);
        if (result instanceof List || result instanceof Map) {
            return new HazelcastJsonValue(serialize(result));
        } else {
            return convertPrimitiveType(result);
        }
    }

    private String extractPath(Object pathArg) {
        if (pathArg instanceof Number) {
            return "$[" + pathArg + "]";
        } else if (pathArg instanceof String) {
            return "$." + pathArg;
        } else {
            throw QueryException.error("INFIX_JSON_FIELD_OPERATOR wrong path argument type provided.");
        }
    }

    private String serialize(Object value) {
        try {
            return SERIALIZER.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw QueryException.error("Failed to serialize INFIX_JSON_FIELD_OPERATOR result: ", exception);
        }
    }

    private Object convertPrimitiveType(Object result) {
        if (!(result instanceof Number)) {
            return result;
        }

        if (result instanceof Float || result instanceof Double) {
            return result;
        }

        final Number number = (Number) result;
        final long value = ((Number) result).longValue();

        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            return number.byteValue();
        } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            return number.shortValue();
        } else if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return number.intValue();
        } else {
            return value;
        }
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.OBJECT;
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.INFIX_JSON_FIELD;
    }
}
