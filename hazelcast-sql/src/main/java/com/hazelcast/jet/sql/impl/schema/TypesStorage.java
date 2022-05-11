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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.FieldsUtil;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class TypesStorage extends TablesStorage {

    public TypesStorage(final NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    public Type getTypeByClass(final Class<?> typeClass) {
        return super.getAllTypes().stream()
                .filter(type -> type.getJavaClassName().equals(typeClass.getName()))
                .findFirst()
                .orElse(null);
    }

    public void registerType(final String name, final Class<?> typeClass) {
        if (getTypeByClass(typeClass) != null || getType(name) != null) {
            return;
        }

        final Type type = new Type();
        type.setName(name);
        type.setJavaClassName(typeClass.getName());
        type.setQueryDataType(new QueryDataType(name, typeClass.getName()));

        if (typeClass.isAssignableFrom(GenericRecord.class)) {
            type.setFields(getFieldsFromGenericRecord(typeClass));
        } else {
            type.setFields(getFieldsFromJavaClass(typeClass, type));
        }

        put(name.toLowerCase(Locale.ROOT), type);
        fixTypeReferences(type);
    }

    public void clear() {
        // TODO: Consistency
        getAllTypes().stream().map(Type::getName).forEach(this::removeType);
    }

    private void fixTypeReferences(final Type addedType) {
        // TODO: type system consistency.
        for (final Type type : getAllTypes()) {
            for (final Type.TypeField field : type.getFields()) {
                if (field.getQueryDataType() == null && !field.getClassName().isEmpty()) {
                    if (addedType.getJavaClassName().equals(field.getClassName())) {
                        field.setQueryDataType(addedType.getQueryDataType());
                    }
                }
            }
        }
    }

    private List<Type.TypeField> getFieldsFromJavaClass(final Class<?> typeClass, final Type thisType) {
        return FieldsUtil.resolveClass(typeClass).entrySet().stream()
                .map(entry -> {
                    final QueryDataType queryDataType;
                    if (isJavaClass(entry.getValue())) {
                        if (entry.getValue().getName().equals(thisType.getJavaClassName())) {
                            queryDataType = thisType.getQueryDataType();
                        } else {
                            final Type existingType = getTypeByClass(entry.getValue());
                            if (existingType != null) {
                                queryDataType = existingType.getQueryDataType();
                            } else {
                                queryDataType = null;
                            }
                        }

                        if (queryDataType == null) {
                            return new Type.TypeField(entry.getKey(), entry.getValue().getName());
                        }
                    } else {
                        queryDataType = QueryDataTypeUtils.resolveTypeForClass(entry.getValue());
                    }

                    return new Type.TypeField(entry.getKey(), queryDataType);
                })
                .collect(Collectors.toList());
    }

    private List<Type.TypeField> getFieldsFromGenericRecord(final Class<?> typeClass) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private boolean isJavaClass(Class<?> clazz) {
        return !clazz.isPrimitive() && !clazz.getPackage().getName().startsWith("java.");
    }
}
