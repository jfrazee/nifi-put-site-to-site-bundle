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

import java.io.*;
import java.util.*;
import javax.net.ssl.SSLContext;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;

@EventDriven
@SupportsBatching
@Tags({"site-to-site"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Site-to-site file transfer to a remote URL and " +
    "port specified via properties or FlowFile attributes")
public class PutSiteToSite extends AbstractProcessor {

    public static final PropertyDescriptor REMOTE_URL = new PropertyDescriptor
            .Builder().name("Remote URL")
            .description("URL of the remote NiFi instance")
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor REMOTE_INPUT_PORT = new PropertyDescriptor
            .Builder().name("Remote Input Port")
            .description("Name of the remote input port")
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USE_COMPRESSION = new PropertyDescriptor
            .Builder().name("Use Compression")
            .description(
                "Specifies whether or not data should be compressed before " +
                "being transferred to or from the remote instance."
            )
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description(
                "The SSL Context Service used to provide client certificate " +
                "information for TLS/SSL enabled site-to-site connections."
            )
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("Client Auth")
            .description(
                "The client authentication policy to use for the SSL " +
                "Context. Only used if an SSL Context Service is provided."
            )
            .required(false)
            .allowableValues(SSLContextService.ClientAuth.values())
            .defaultValue(SSLContextService.ClientAuth.REQUIRED.name())
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

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private SiteToSiteClient getClient(String url, String port, boolean useCompression, SSLContext sslContext) {
      SiteToSiteClient.Builder configBuilder = new SiteToSiteClient.Builder()
          .url(url)
          .portName(port)
          .useCompression(useCompression);

      if (sslContext != null) {
          configBuilder = configBuilder.sslContext(sslContext);
      }

      final SiteToSiteClientConfig config = configBuilder.buildConfig();

      final SiteToSiteClient client = new SiteToSiteClient.Builder()
         .fromConfig(config)
         .build();

      return client;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(REMOTE_URL);
        descriptors.add(REMOTE_INPUT_PORT);
        descriptors.add(USE_COMPRESSION);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(CLIENT_AUTH);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
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

        final String remoteUrl = context.getProperty(REMOTE_URL)
            .evaluateAttributeExpressions(flowFile)
            .getValue();

        final String remoteInputPort = context.getProperty(REMOTE_INPUT_PORT)
            .evaluateAttributeExpressions(flowFile)
            .getValue();

        final String useCompressionStr = context.getProperty(USE_COMPRESSION).getValue();
        final boolean useCompression = (useCompressionStr == null) ? false : useCompressionStr.equals("true");

        final String clientAuth = context.getProperty(CLIENT_AUTH).getValue();

        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE)
            .asControllerService(SSLContextService.class);

        final SSLContext sslContext = (sslContextService == null) ? null : sslContextService.createSSLContext(SSLContextService.ClientAuth.valueOf(clientAuth));

        try {
          final SiteToSiteClient client = getClient(remoteUrl, remoteInputPort, useCompression, sslContext);

          try {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);

            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            session.exportTo(flowFile, bytes);
            final byte[] data = bytes.toByteArray();
            final Map<String, String> attributes = flowFile.getAttributes();

            transaction.send(data, attributes);
            transaction.confirm();
            transaction.complete();
          }
          finally {
            try { client.close(); } catch (Exception e) { }
          }
        }
        catch (Exception e) {
          getLogger().error("Site-to-site transfer to {} at {} failed for FlowFile {}", new Object[]{remoteInputPort, remoteUrl, flowFile}, e);
          flowFile = session.penalize(flowFile);
          session.transfer(flowFile, REL_FAILURE);
          return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
