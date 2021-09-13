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

package com.hazelcast.jet.sql.impl.parse;

import com.hazelcast.jet.sql.impl.validate.types.HazelcastJsonType;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

public class SqlInfixJsonFieldOperator extends SqlBinaryOperator {
    public static final SqlOperator INSTANCE = new SqlInfixJsonFieldOperator();
    private static final int PRECEDENCE = 94;

    public SqlInfixJsonFieldOperator() {
        super("->",
                SqlKind.OTHER,
                PRECEDENCE,
                true,
                (opBinding -> opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY)),
                ((callBinding, returnType, operandTypes) -> {
                    operandTypes[0] = HazelcastJsonType.create(callBinding.getOperandType(0).isNullable());
                    operandTypes[1] = callBinding.getTypeFactory()
                            .createSqlType(callBinding.getOperandType(1).getSqlTypeName());
                }),
                null);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public boolean checkOperandTypes(final SqlCallBinding callBinding, final boolean throwOnFailure) {
        return true;
    }
}
