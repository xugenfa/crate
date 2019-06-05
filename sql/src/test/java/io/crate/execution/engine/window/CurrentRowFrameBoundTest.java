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

package io.crate.execution.engine.window;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import static io.crate.sql.tree.FrameBound.Type.CURRENT_ROW;
import static org.hamcrest.core.Is.is;

public class CurrentRowFrameBoundTest extends CrateUnitTest {

    @Test
    public void testCurrentRowEndIsFirstNonPeer() {
        // partition: 1, 1, 2
        int firstFrameEnd = CURRENT_ROW.getEnd(0, 3, 0, true, (pos1, pos2) -> 2);
        assertThat(firstFrameEnd, is(2));
        int secondFrameEnd = CURRENT_ROW.getEnd(0, 3, 1, true, (pos1, pos2) -> 3);
        assertThat(secondFrameEnd, is(3));
    }

    @Test
    public void testCurrentRowStartForOrderedPartition() {
        // partition: 1, 1, 2
        int firstFrameStart = CURRENT_ROW.getStart(0, 3, 0, 0, true, (pos1, pos2) -> true);
        assertThat(firstFrameStart, is(0));
        int secondFrameStart = CURRENT_ROW.getStart(0,3,  0, 1, true, (pos1, pos2) -> true);
        assertThat("a new frame starts when encountering a non-peer", secondFrameStart, is(0));
        int thirdFrameStart = CURRENT_ROW.getStart(0,3,  0, 2, true, (pos1, pos2) -> false);
        assertThat(thirdFrameStart, is(2));
    }

    @Test
    public void testCurrentRowStartIsTheRowIdForUnorderedPartitions() {
        // partition: 1, 1, 2
        int firstFrameStart = CURRENT_ROW.getStart(0, 3, 0, 0, false, (pos1, pos2) -> true);
        assertThat(firstFrameStart, is(0));
        int secondFrameStart = CURRENT_ROW.getStart(0, 3, 0, 1, false, (pos1, pos2) -> true);
        assertThat(secondFrameStart, is(1));
        int thirdFrameStart = CURRENT_ROW.getStart(0, 3, 1, 2, false, (pos1, pos2) -> true);
        assertThat(thirdFrameStart, is(2));
    }

}