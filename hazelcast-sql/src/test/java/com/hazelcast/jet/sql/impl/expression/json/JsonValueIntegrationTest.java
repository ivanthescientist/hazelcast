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
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * A test for JSON_VALUE function.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonValueIntegrationTest extends SqlJsonTestSupport {

    @BeforeClass
    public static void beforeClass() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);
        initialize(1, config);
    }

    @Test
    public void when_calledWithBasicSyntax_objectValueIsReturned() {
        initMultiTypeObject();
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");
        assertRowsWithType("SELECT JSON_VALUE(this, '$.byteField') FROM test" ,
                singletonList(SqlColumnType.VARCHAR), rows(1, "1"));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.shortField') FROM test" ,
                singletonList(SqlColumnType.VARCHAR), rows(1, "2"));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.intField') FROM test" ,
                singletonList(SqlColumnType.VARCHAR), rows(1, "3"));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.longField') FROM test" ,
                singletonList(SqlColumnType.VARCHAR), rows(1, "4"));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.stringField') FROM test" ,
                singletonList(SqlColumnType.VARCHAR), rows(1, "6"));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.charField') FROM test" ,
                singletonList(SqlColumnType.VARCHAR), rows(1, "7"));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.floatField') FROM test" ,
                singletonList(SqlColumnType.VARCHAR), rows(1, "8.0"));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.doubleField') FROM test" ,
                singletonList(SqlColumnType.VARCHAR), rows(1, "9.0"));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.bigDecimalField') FROM test" ,
                singletonList(SqlColumnType.VARCHAR), rows(1, "1E+1000"));
    }

    @Test
    public void when_calledWithReturning_correctTypeIsReturned() {
        initMultiTypeObject();
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");
        assertRowsWithType("SELECT JSON_VALUE(this, '$.byteField' RETURNING TINYINT) FROM test" ,
                singletonList(SqlColumnType.TINYINT), rows(1, (byte) 1));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.shortField' RETURNING SMALLINT) FROM test" ,
                singletonList(SqlColumnType.SMALLINT), rows(1, (short) 2));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.intField' RETURNING INTEGER) FROM test" ,
                singletonList(SqlColumnType.INTEGER), rows(1, 3));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.longField' RETURNING BIGINT) FROM test" ,
                singletonList(SqlColumnType.BIGINT), rows(1, 4L));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.stringField' RETURNING VARCHAR) FROM test" ,
                singletonList(SqlColumnType.VARCHAR), rows(1, "6"));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.charField' RETURNING VARCHAR) FROM test" ,
                singletonList(SqlColumnType.VARCHAR), rows(1, "7"));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.floatField' RETURNING REAL) FROM test" ,
                singletonList(SqlColumnType.REAL), rows(1, 8.0f));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.doubleField' RETURNING DOUBLE) FROM test" ,
                singletonList(SqlColumnType.DOUBLE), rows(1, 9.0));
        assertRowsWithType("SELECT JSON_VALUE(this, '$.bigDecimalField' RETURNING DECIMAL) FROM test" ,
                singletonList(SqlColumnType.DECIMAL), rows(1, new BigDecimal("1e1000")));
    }

    @Test
    public void when_extendedSyntaxIsSpecified_queryWorksCorrectly() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, json(""));
        test.put(2L, json("[1,2,"));
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");

        assertNull(querySingleValue("SELECT JSON_VALUE(this, '$' NULL ON EMPTY) AS c1 FROM test WHERE __key = 1"));
        assertEquals((byte) 1, querySingleValue("SELECT JSON_VALUE(this, '$' DEFAULT 1 ON EMPTY) AS c1 FROM test WHERE __key = 1"));
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(this, '$' ERROR ON EMPTY) AS c1 FROM test WHERE __key = 1"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSON argument is empty");

        assertNull(querySingleValue("SELECT JSON_VALUE(this, '$' NULL ON ERROR) FROM test WHERE __key = 2"));
        assertEquals((byte) 1, querySingleValue("SELECT JSON_VALUE(this, '$' DEFAULT 1 ON ERROR) AS c1 FROM test WHERE __key = 2"));
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(this, '$' ERROR ON ERROR) FROM test WHERE __key = 2"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSON_VALUE failed");
    }

    @Test
    public void when_fullExtendedSyntaxIsSpecifiedQueryWorks() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, json(""));
        test.put(2L, json("[1,2,"));
        test.put(3L, json("[3]"));
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");

        assertRowsWithType(
                "SELECT __key, JSON_VALUE(this, '$[0]' RETURNING BIGINT DEFAULT CAST(1 AS BIGINT) ON EMPTY DEFAULT CAST(2 AS BIGINT) ON ERROR) FROM test",
                Arrays.asList(SqlColumnType.BIGINT, SqlColumnType.BIGINT),
                rows(2, 1L, 1L, 2L, 2L, 3L, 3L)
        );
    }

    @Test
    public void when_arrayIsReturned_errorIsThrown() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, json("[1,2,3]"));
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");

        assertNull(querySingleValue("SELECT JSON_VALUE(this, '$' NULL ON ERROR) FROM test"));
        assertEquals((byte) 1, querySingleValue("SELECT JSON_VALUE(this, '$' DEFAULT 1 ON ERROR) FROM test"));
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(this, '$' ERROR ON ERROR) FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Result of JSON_VALUE can not be array or object");
    }

    @Test
    public void when_objectIsReturned_errorIsThrown() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, json("{\"test\":1}"));
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");

        assertNull(querySingleValue("SELECT JSON_VALUE(this, '$' NULL ON ERROR) FROM test"));
        assertEquals((byte) 1, querySingleValue("SELECT JSON_VALUE(this, '$' DEFAULT 1 ON ERROR) FROM test"));
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(this, '$' ERROR ON ERROR) FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Result of JSON_VALUE can not be array or object");
    }

    private void initMultiTypeObject() {
        final MultiTypeObject value = new MultiTypeObject();
        final String serializedValue;
        try {
            serializedValue = SERIALIZER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new HazelcastException(e);
        }
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, new HazelcastJsonValue(serializedValue));
    }

    private static class MultiTypeObject {
        public Byte byteField = 1;
        public Short shortField = 2;
        public Integer intField = 3;
        public Long longField = 4L;
        public String stringField = "6";
        public Character charField = '7';
        public Float floatField = 8.0f;
        public Double doubleField = 9.0;
        public BigDecimal bigDecimalField = new BigDecimal("1e1000");
    }
}
