/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.joeyfrazee.nifi.processors;

import java.util.*;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.MockFlowFile;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestDuplicateByAttribute {

    private TestRunner runner;

    @Before
    public void init() {
        runner = TestRunners.newTestRunner(DuplicateByAttribute.class);
    }

    @Test
    public void testSuccess() {
        runner.setProperty(DuplicateByAttribute.ATTRIBUTE_TO_DUPLICATE_BY, "list_of_things");
        runner.setProperty(DuplicateByAttribute.OUTPUT_ATTRIBUTE, "thing");

        final Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("list_of_things", "lions,tigers,bears");

        runner.enqueue("some content".getBytes(), attributes);
        runner.run();

        runner.assertTransferCount(DuplicateByAttribute.REL_SUCCESS, 3);
        runner.assertTransferCount(DuplicateByAttribute.REL_FAILURE, 0);
        runner.assertTransferCount(DuplicateByAttribute.REL_ORIGINAL, 1);

        final Set<String> expected = new HashSet<String>();
        expected.add("lions");
        expected.add("tigers");
        expected.add("bears");

        final Set<String> actual = new HashSet<String>();
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(DuplicateByAttribute.REL_SUCCESS)) {
            flowFile.assertAttributeExists("thing");
            flowFile.assertAttributeNotExists("list_of_things");
            flowFile.assertContentEquals("some content");

            final String thing = flowFile.getAttribute("thing");
            if (thing != null) {
                actual.add(thing);
            }
        }

        assertEquals(actual, expected);

        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(DuplicateByAttribute.REL_ORIGINAL)) {
            flowFile.assertAttributeExists("list_of_things");
            flowFile.assertAttributeEquals("list_of_things", "lions,tigers,bears");
            flowFile.assertAttributeNotExists("thing");
            flowFile.assertContentEquals("some content");
        }
    }

    @Test
    public void testFailure() {
        runner.setProperty(DuplicateByAttribute.ATTRIBUTE_TO_DUPLICATE_BY, "list_of_things");
        runner.setProperty(DuplicateByAttribute.OUTPUT_ATTRIBUTE, "thing");

        final Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("list_of_things", "\"lions,\"tigers\",\"bears\"");

        runner.enqueue("some content".getBytes(), attributes);
        runner.run();

        runner.assertTransferCount(DuplicateByAttribute.REL_SUCCESS, 0);
        runner.assertTransferCount(DuplicateByAttribute.REL_FAILURE, 1);
        runner.assertTransferCount(DuplicateByAttribute.REL_ORIGINAL, 0);

        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(DuplicateByAttribute.REL_FAILURE)) {
            flowFile.assertAttributeExists("list_of_things");
            flowFile.assertAttributeEquals("list_of_things", "\"lions,\"tigers\",\"bears\"");
            flowFile.assertAttributeNotExists("thing");
            flowFile.assertContentEquals("some content");
        }
    }

}
