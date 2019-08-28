/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.optimizer.rule;

import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

import java.util.List;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;

public final class FetchRewriteEvalLimitCollect implements Rule<Eval> {

    private final Capture<Limit> limit;
    private final Capture<Collect> collect;
    private final Pattern<Eval> pattern;

    public FetchRewriteEvalLimitCollect() {
        this.limit = new Capture<>();
        this.collect = new Capture<>();
        /**
         * We'd need separate rules for:
         *      Eval - Limit - Order - Collect
         *      Eval - Order - Limit - Collect
         *
         *      Eval
         *          (Limit, Order) ?
         *      HashJoin
         *          - Collect
         *          - Collect
         *
         *      Eval
         *          (Limit, Order) ?
         *      NestedLoop
         *          - Collect
         *          - Collect
         *
         *  What about ProjectSet, WindowAgg?
         */
        this.pattern = typeOf(Eval.class)
            .with(
                source(),
                typeOf(Limit.class).capturedAs(limit)
                    .with(source(), typeOf(Collect.class).capturedAs(collect))
            );
    }

    @Override
    public Pattern<Eval> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Eval eval, Captures captures) {
        Collect collect = captures.get(this.collect);
        AbstractTableRelation tableRelation = collect.baseTables().get(0);
        if (!(tableRelation instanceof DocTableRelation)) {
            return null;
        }
        Collect collectFetchId = new Collect(
            collect.preferSourceLookup(),
            tableRelation,
            List.of(DocSysColumns.forTable(tableRelation.tableInfo().ident(), DocSysColumns.FETCHID)),
            collect.where(),
            collect.numExpectedRows(),
            collect.estimatedRowSize()
        );
        Limit limit = captures.get(this.limit);
        /*
        return new Fetch(
            limit.replaceSources(List.of(collectFetchId)),
            eval.outputs()
        );
         */
        return null;
    }
}
