/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.sql.tree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.crate.sql.ExpressionFormatter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;


public class CheckConstraint<T> extends TableElement<T> implements ToXContent {

    private static final ConcurrentMap<String, AtomicInteger> CHECK_SUFFIXES = new ConcurrentHashMap<>();

    private static String uniqueName(String tableName, String columnName) {
        if (null == tableName || tableName.isEmpty()) {
            throw new IllegalArgumentException("tableName cannot be null or empty");
        }
        StringBuilder sb = new StringBuilder(tableName);
        if (null != columnName && false == columnName.isEmpty()) {
            sb.append("_").append(columnName);
        }
        sb.append("_check");
        String prefix = sb.toString();
        AtomicInteger count = CHECK_SUFFIXES.computeIfAbsent(prefix, k -> new AtomicInteger(1));
        if (1 == count.get()) {
            return prefix;
        }
        return sb.append("_").append(count.getAndIncrement()).toString();
    }


    private String name;
    private final Expression expression;

    public CheckConstraint(@Nullable String name, Expression expression) {
        if (null != name && CHECK_SUFFIXES.containsKey(name)) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH, "a check constraint [%s] already exists", name));
        }
        this.name = name;
        this.expression = expression;
    }

    public void setName(String tableName) {
        setName(tableName, null);
    }

    public void setName(String tableName, String columnName) {
        this.name = uniqueName(tableName, columnName);
    }

    public String name() {
        return name;
    }

    public Expression expression() {
        return expression;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, expression);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (null == o || false == o instanceof CheckConstraint) {
            return false;
        }
        CheckConstraint that = (CheckConstraint) o;
        return Objects.equal(expression, that.expression) &&
               Objects.equal(name, that.name);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("expression", ExpressionFormatter.formatStandaloneExpression(expression))
            .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCheckConstraint(this, context);
    }

    @Override
    public <U> TableElement<U> map(Function<? super T, ? extends U> mapper) {
        return new CheckConstraint<>(name, expression);
    }

    @Override
    public void visit(Consumer<? super T> consumer) {
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(name, ExpressionFormatter.formatStandaloneExpression(expression));
        builder.endObject();
        return null;
    }
}
