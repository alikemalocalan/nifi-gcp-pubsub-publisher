
package com.synack.nifi.gcp.pubsub.publisher;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.threeten.bp.Duration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

/**
 * @author Mikhail Sosonkin
 */
@Tags({"gcp", "publisher", "publish"})
@CapabilityDescription("Publish to a GCP Pubsub topic")
@SeeAlso({})
@ReadsAttributes({
        @ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "name of the flow based on time"),
        @WritesAttribute(attribute = "ack_id", description = "GCP meassge ACK id")})
public class GcpPubsubPublisher extends AbstractProcessor {

    public static final PropertyDescriptor authProperty = new PropertyDescriptor.Builder().name("Authentication Keys")
            .description("Required if outside of GCP. OAuth token (contents of myproject.json)")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor topicProperty = new PropertyDescriptor.Builder().name("Topic")
            .description("Name of topic")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor projectIdProperty = new PropertyDescriptor.Builder().name("Project ID")
            .description("Project ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor batchProperty = new PropertyDescriptor.Builder().name("Batch size")
            .description("Max number of messages to send at a time")
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be published")
            .build();

    static final Relationship REL_TOOBIG = new Relationship.Builder()
            .name("toobig")
            .description("FlowFiles that are too big to be published")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    private Publisher publisher;
    private static int MAX_FLOW_SIZE = 9000000;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(authProperty);
        descriptors.add(topicProperty);
        descriptors.add(projectIdProperty);
        descriptors.add(batchProperty);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_TOOBIG);
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
        Long batchSize = Long.valueOf(context.getProperty(batchProperty).getValue());

        ProjectTopicName topic = ProjectTopicName.of(context.getProperty(projectIdProperty).getValue(),
                context.getProperty(topicProperty).getValue());
        try {
            PropertyValue authKeys = context.getProperty(authProperty);
            if (authKeys.isSet()) {
                publisher = createPublisherWithCustomCredentials(topic, authKeys.getValue(), batchSize);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        /*
        if (publisher == null || topic == null) {
            throw new ProcessException("Context not initialized");
        }
        */

        // do the normal flow stuff.
        int batch = context.getProperty(batchProperty).asLong().intValue();
        int counts = session.getQueueSize().getObjectCount();
        counts = Math.min(batch, counts);

        // get as many messages as we can
        List<FlowFile> flowFiles = session.get(counts);
        if (flowFiles.size() == 0) {
            return;
        }

        long totalSize = 0;
        List<FlowFile> toProcess = new ArrayList<>(counts);
        for (FlowFile flowFile : flowFiles) {
            if (flowFile.getSize() > MAX_FLOW_SIZE) {
                session.transfer(flowFile, REL_TOOBIG);
            } else {
                totalSize += flowFile.getSize();

                if (totalSize < MAX_FLOW_SIZE) {
                    toProcess.add(flowFile);
                } else {
                    session.transfer(flowFile);
                }
            }
        }

        // obtain the contents
        for (FlowFile flowFile : toProcess) {
            session.read(flowFile, in -> {
                ByteString data = ByteString.copyFrom(IOUtils.toByteArray(in));
                publisher.publish(PubsubMessage.newBuilder().setData(data).build());
            });
        }

        // upload the messages and clean up local flows.
        for (FlowFile flowFile : toProcess) {
            session.remove(flowFile);
        }

        session.commit();

    }

    private Publisher createPublisherWithCustomCredentials(ProjectTopicName topicName, String authKeyStream, Long batchSize) throws IOException {
        // [START pubsub_publisher_custom_credentials]
        // read service account credentials from file
        long requestBytesThreshold = 5000L; // default : 1kb
        Duration publishDelayThreshold = Duration.ofMillis(100); // default : 1 ms


        CredentialsProvider credentialsProvider =
                FixedCredentialsProvider.create(
                        ServiceAccountCredentials.fromStream(new ByteArrayInputStream(authKeyStream.getBytes())));

        BatchingSettings batchingSettings = BatchingSettings.newBuilder()
                .setElementCountThreshold(batchSize)
                .setRequestByteThreshold(requestBytesThreshold)
                .setDelayThreshold(publishDelayThreshold)
                .build();


        // [END pubsub_publisher_custom_credentials]
        return Publisher.newBuilder(topicName)
                .setBatchingSettings(batchingSettings)
                .setCredentialsProvider(credentialsProvider)
                .build();
    }
}
