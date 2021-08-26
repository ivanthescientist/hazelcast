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
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonQueryIntegrationTest extends SqlJsonTestSupport {
    @BeforeClass
    public static void beforeClass() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);
        initialize(1, config);
    }

    @Test
    public void when_stringIsPassed_queryWorks() {
        final IMap<Long, String> test = instance().getMap("test");
        test.put(1L, "[1,2,3]");
        for (final SqlRow row : instance().getSql().execute("SELECT JSON_QUERY(this, '$[?(@ > 1)]') FROM test")) {
            System.out.println(row);
            assertEquals(SqlColumnType.JSON, row.getMetadata().getColumn(0).getType());
            assertEquals(new HazelcastJsonValue("[2,3]"), row.getObject(0));
        }
    }

    @Test
    public void when_jsonIsPassed_queryWorks() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, new HazelcastJsonValue("[1,2,3]"));
        for (final SqlRow row : instance().getSql().execute("SELECT JSON_QUERY(this, '$[?(@ > 1)]') FROM test")) {
            System.out.println(row);
            assertEquals(SqlColumnType.JSON, row.getMetadata().getColumn(0).getType());
            assertEquals(new HazelcastJsonValue("[2,3]"), row.getObject(0));
        }
    }

    @Test
    public void when_mappedJsonIsPassed_queryWorks() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, new HazelcastJsonValue("[1,2,3]"));
        instance().getSql()
                .execute("CREATE MAPPING test (__key BIGINT, this JSON) " +
                        "TYPE IMap " +
                        "OPTIONS ('keyFormat'='bigint', 'valueFormat'='json_type')")
                .updateCount();

        for (final SqlRow row : instance().getSql().execute("SELECT JSON_QUERY(this, '$') FROM test")) {
            System.out.println(row);
            assertEquals(SqlColumnType.JSON, row.getMetadata().getColumn(0).getType());
            assertEquals(new HazelcastJsonValue("[1,2,3]"), row.getObject(0));
        }
    }

    @Test
    public void when_complexObjectIsPassed_queryWorks() {
        final IMap<Long, ComplexObject> test = instance().getMap("test");
        test.put(1L, new ComplexObject(1L, "[1,2,3]"));
        for (final SqlRow row : instance().getSql().execute("SELECT JSON_QUERY(jsonValue, '$'), id FROM test")) {
            System.out.println(row);
            assertEquals(SqlColumnType.JSON, row.getMetadata().getColumn(0).getType());
            assertEquals(new HazelcastJsonValue("[1,2,3]"), row.getObject(0));
            assertEquals(SqlColumnType.BIGINT, row.getMetadata().getColumn(1).getType());
            assertEquals((Long) 1L, row.getObject(1));
        }
    }

    @Test
    public void when_extendedSyntaxSpecified_queryWorks() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, new HazelcastJsonValue(""));
        test.put(2L, new HazelcastJsonValue("[1,2,"));

        assertThrows(HazelcastSqlException.class, () -> instance().getSql()
                .execute("SELECT JSON_QUERY(this, '$' ERROR ON EMPTY) AS c1 FROM test WHERE __key = 1"));
        assertNull(querySingleValue("SELECT JSON_QUERY(this, '$' NULL ON EMPTY) AS c1 FROM test WHERE __key = 1"));
        assertEquals(new HazelcastJsonValue("[]"),
                querySingleValue("SELECT JSON_QUERY(this, '$' EMPTY ARRAY ON EMPTY) AS c1 FROM test WHERE __key = 1"));
        assertEquals(new HazelcastJsonValue("{}"),
                querySingleValue("SELECT JSON_QUERY(this, '$' EMPTY OBJECT ON EMPTY) AS c1 FROM test WHERE __key = 1"));

        assertThrows(HazelcastSqlException.class, () -> instance().getSql()
                .execute("SELECT JSON_QUERY(this, '$' ERROR ON ERROR) AS c1 FROM test WHERE __key = 2"));
        assertNull(querySingleValue("SELECT JSON_QUERY(this, '$' NULL ON ERROR) AS c1 FROM test WHERE __key = 2"));
        assertEquals(new HazelcastJsonValue("[]"),
                querySingleValue("SELECT JSON_QUERY(this, '$' EMPTY ARRAY ON ERROR) AS c1 FROM test WHERE __key = 2"));
        assertEquals(new HazelcastJsonValue("{}"),
                querySingleValue("SELECT JSON_QUERY(this, '$' EMPTY OBJECT ON ERROR) AS c1 FROM test WHERE __key = 2"));
    }

    @Test
    public void when_defaultWrapperBehaviorIsSpecified_queryWorks() {
        initComplexObject();
        assertEquals("[1,\"2\",3,{\"t\":1}]",
                querySingleValue("SELECT JSON_QUERY(this, '$[0]') FROM test").toString());
        assertEquals("{\"t\":1}",
                querySingleValue("SELECT JSON_QUERY(this, '$[1]') FROM test").toString());
        assertThrows(HazelcastSqlException.class,
                () -> querySingleValue("SELECT JSON_QUERY(this, '$[2]' ERROR ON ERROR) FROM test"));
    }

    @Test
    public void when_noArrayWrapperSpecified_queryWorks() {
        initComplexObject();
        assertEquals("[1,\"2\",3,{\"t\":1}]",
                querySingleValue("SELECT JSON_QUERY(this, '$[0]' WITHOUT ARRAY WRAPPER) FROM test")
                        .toString());
        assertEquals("{\"t\":1}",
                querySingleValue("SELECT JSON_QUERY(this, '$[1]' WITHOUT ARRAY WRAPPER) FROM test")
                        .toString());
        assertThrows(HazelcastSqlException.class,
                () -> querySingleValue("SELECT JSON_QUERY(this, '$[2]' WITHOUT ARRAY WRAPPER ERROR ON ERROR) "
                        + "FROM test"));
    }

    @Test
    public void when_conditionalArrayWrapperSpecified_queryWorks() {
        initComplexObject();
        assertEquals("[1,\"2\",3,{\"t\":1}]",
                querySingleValue("SELECT JSON_QUERY(this, '$[0]' WITH CONDITIONAL ARRAY WRAPPER) FROM test")
                        .toString());
        assertEquals("{\"t\":1}",
                querySingleValue("SELECT JSON_QUERY(this, '$[1]' WITH CONDITIONAL ARRAY WRAPPER) FROM test")
                        .toString());
        assertEquals("[3]",
                querySingleValue("SELECT JSON_QUERY(this, '$[2]' WITH CONDITIONAL ARRAY WRAPPER ERROR ON ERROR) FROM test")
                        .toString());
    }

    @Test
    public void when_unconditionalArrayWrapperSpecified_queryWorks() {
        initComplexObject();
        assertEquals("[[1,\"2\",3,{\"t\":1}]]",
                querySingleValue("SELECT JSON_QUERY(this, '$[0]' WITH UNCONDITIONAL ARRAY WRAPPER) FROM test")
                        .toString());
        assertEquals("[{\"t\":1}]",
                querySingleValue("SELECT JSON_QUERY(this, '$[1]' WITH UNCONDITIONAL ARRAY WRAPPER) FROM test")
                        .toString());
        assertEquals("[3]",
                querySingleValue("SELECT JSON_QUERY(this, '$[2]' WITH UNCONDITIONAL ARRAY WRAPPER ERROR ON ERROR) FROM test")
                        .toString());
    }
}
