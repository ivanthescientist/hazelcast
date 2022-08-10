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

package com.hazelcast.jet.sql.impl.expression;

import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.expression.UniExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ToRowFunction extends UniExpressionWithType<RowValue> implements IdentifiedDataSerializable {

    public ToRowFunction() { }

    private ToRowFunction(Expression<?> operand) {
        super(operand, QueryDataType.ROW);
    }

    public static ToRowFunction create(Expression<?> operand) {
        return new ToRowFunction(operand);
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.TO_ROW;
    }

    @Override
    public RowValue eval(final Row row, final ExpressionEvalContext context) {
        final Object object = this.operand.eval(row, context);
        final QueryDataType queryDataType = operand.getType();

        return convert(object, queryDataType, new HashSet<>());
    }

    private RowValue convert(final Object obj, final QueryDataType dataType, final Set<Integer> foundObjects) {
        // TODO: Compact and Portable support
        foundObjects.add(System.identityHashCode(obj));

        final List<Object> fieldValues = new ArrayList<>();
        for (final QueryDataType.QueryDataTypeField field : dataType.getObjectFields()) {
            final Object fieldValue = ReflectionUtils.getFieldValue(field.getName(), obj);
            if (!field.getDataType().isCustomType() || fieldValue == null) {
                fieldValues.add(fieldValue);
                continue;
            }
            if (!foundObjects.contains(System.identityHashCode(fieldValue))) {
                    fieldValues.add(convert(fieldValue, field.getDataType(), foundObjects));
            }
        }
        return new RowValue(fieldValues);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.ROW;
    }
}
