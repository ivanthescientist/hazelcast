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

package com.hazelcast.jet.sql.impl.validate.operators.json;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastJsonType;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

public class HazelcastParseJsonFunction extends HazelcastFunction {
    public static final HazelcastParseJsonFunction INSTANCE = new HazelcastParseJsonFunction();
    public HazelcastParseJsonFunction() {
        super(
                "PARSE_JSON",
                SqlKind.OTHER_FUNCTION,
                opBinding -> HazelcastJsonType.create(false),
                new ReplaceUnknownOperandTypeInference(SqlTypeName.VARCHAR),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    protected boolean checkOperandTypes(final HazelcastCallBinding callBinding, final boolean throwOnFailure) {
        return TypedOperandChecker.VARCHAR.check(callBinding, throwOnFailure, 0);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(1);
    }
}
