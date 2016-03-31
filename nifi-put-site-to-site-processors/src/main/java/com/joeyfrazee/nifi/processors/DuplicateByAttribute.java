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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

@EventDriven
@SupportsBatching
@Tags({"duplicate", "clone", "copy", "fan-out"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Duplicates, clones, copies or fans-out FlowFiles " +
    "according to a comma-separated list. Along with the expression " +
    "language, this can be used to send FlowFiles to multiple destinations " +
    "specified in a dynamic attribute, ExecuteSQL resultset, etc.")
public class DuplicateByAttribute extends AbstractProcessor {

    public static final PropertyDescriptor ATTRIBUTE_TO_DUPLICATE_BY = new PropertyDescriptor
            .Builder().name("Attribute to Duplicate By")
            .description(
                "The name of the attribute to duplicate by. The contents of " +
                "this attribute should be a comma-separated list of values. " +
                "Each value will be saved as a new attribute with the name " +
                "specified in 'Output Attribute'."
            )
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_ATTRIBUTE = new PropertyDescriptor
            .Builder().name("Output Attribute")
            .description(
                "The name of the attribute that will be written from the " +
                "contents of the 'Attribute to Duplicate By' attribute"
            )
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description(
                "Any FlowFile that is successfully transferred is routed to " +
                "this relationship"
            )
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                "Any FlowFile that fails to be transferred is routed to " +
                "this relationship"
            )
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description(
                "The original file is always routed to this relationship"
            )
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ATTRIBUTE_TO_DUPLICATE_BY);
        descriptors.add(OUTPUT_ATTRIBUTE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String attributeToDuplicateBy = context.getProperty(ATTRIBUTE_TO_DUPLICATE_BY)
            .evaluateAttributeExpressions(flowFile)
            .getValue();

        String outputAttribute = context.getProperty(OUTPUT_ATTRIBUTE)
            .evaluateAttributeExpressions(flowFile)
            .getValue();

        try {
            final String csv = flowFile.getAttribute(attributeToDuplicateBy);
            final CSVParser parser = CSVParser.parse(csv, CSVFormat.DEFAULT);
            for (final CSVRecord record : parser) {
                for (final String v : record) {
                    FlowFile copy = session.clone(flowFile);
                    copy = session.removeAttribute(copy, attributeToDuplicateBy);
                    copy = session.putAttribute(copy, outputAttribute, v);
                    session.transfer(copy, REL_SUCCESS);
                }
            }
        }
        catch (Exception e) {
            getLogger().error("{} value {} could not be parsed", new Object[]{ATTRIBUTE_TO_DUPLICATE_BY.getName(), attributeToDuplicateBy}, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_ORIGINAL);
    }
}
