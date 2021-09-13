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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.Collections.singletonList;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InfixJsonFieldOperatorIntegrationTest extends SqlJsonTestSupport {
    @BeforeClass
    public static void beforeClass() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);
        initialize(1, config);
    }

    @Test
    public void when_expressionReturnsPrimitiveType_exactValueIsReturned() {
        initComplexObject();
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");
        assertRowsWithType("SELECT this->0->3->'t' FROM test",
                singletonList(SqlColumnType.OBJECT), rows(1, (byte) 1));
        assertRowsWithType("SELECT this->1->'t' FROM test",
                singletonList(SqlColumnType.OBJECT), rows(1, (byte) 1));
        // TODO: fix logic? JsonPath evals "3" as a number instead of string.
        assertRowsWithType("SELECT this->2 FROM test",
                singletonList(SqlColumnType.OBJECT), rows(1,  (byte) 3));
    }

    @Test
    public void when_expressionReturnArray_hzJsonValueIsReturned() {
        initComplexObject();
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");
        assertRowsWithType("SELECT this->0 FROM test",
                singletonList(SqlColumnType.OBJECT), rows(1, new HazelcastJsonValue("[1,\"2\",3,{\"t\":1}]")));
    }

    @Test
    public void when_expressionReturnsObject_hzJsonValueIsReturned() {
        initComplexObject();
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");
        assertRowsWithType("SELECT this->1 FROM test",
                singletonList(SqlColumnType.OBJECT), rows(1, new HazelcastJsonValue("{\"t\":1}")));
    }
}
