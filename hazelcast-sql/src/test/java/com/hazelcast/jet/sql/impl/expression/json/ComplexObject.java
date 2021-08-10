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

import com.hazelcast.core.HazelcastJsonValue;

import java.io.Serializable;

public class ComplexObject implements Serializable {
    private Long id;
    private HazelcastJsonValue jsonValue;

    public ComplexObject() {
    }

    public ComplexObject(final Long id, final HazelcastJsonValue jsonValue) {
        this.id = id;
        this.jsonValue = jsonValue;
    }

    public Long getId() {
        return id;
    }

    public void setId(final Long id) {
        this.id = id;
    }

    public HazelcastJsonValue getJsonValue() {
        return jsonValue;
    }

    public void setJsonValue(final HazelcastJsonValue jsonValue) {
        this.jsonValue = jsonValue;
    }
}
