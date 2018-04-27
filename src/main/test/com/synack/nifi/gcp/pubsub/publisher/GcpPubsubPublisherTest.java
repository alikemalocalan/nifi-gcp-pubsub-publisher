package com.synack.nifi.gcp.pubsub.publisher;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.function.Consumer;

public class GcpPubsubPublisherTest {


    @Test
    public void sendStringToPubSub() {

        //content of credential json file
        String authkey = "";

        // Content to be mock a json file
        InputStream content = new ByteArrayInputStream("test datas".getBytes());
        TestRunner testRunner = TestRunners.newTestRunner(GcpPubsubPublisher.class);

        // Generate a test runner to mock a processor in a flow
        testRunner.setProperty(GcpPubsubPublisher.projectIdProperty, "project-test");
        testRunner.setProperty(GcpPubsubPublisher.topicProperty, "test-topic");
        testRunner.setProperty(GcpPubsubPublisher.authProperty, authkey);
        testRunner.setProperty(GcpPubsubPublisher.batchProperty, "10");

        // Add the content to the runner
        testRunner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        testRunner.run();

        // All results were processed with out failure
        //testRunner.assertQueueEmpty();

        // If you need to read or do aditional tests on results you can access the content
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(GcpPubsubPublisher.SUCCESS);
        results.forEach(new Consumer<MockFlowFile>() {
            @Override
            public void accept(MockFlowFile mockFlowFile) {
                String resultValue = mockFlowFile.toString();
                System.out.println("\n Match: " + resultValue);
                // Test attributes and content
                //result.assertAttributeEquals(NIFIQueryHasher.MATCH_ATTR, "nifi rocks");
                //System.out.println(mockFlowFile.getAttribute(NIFIQueryHasher.MATCH_ATTR));
                //result.assertContentEquals("nifi rocks");
            }
        });

        List<MockFlowFile> results_tobig = testRunner.getFlowFilesForRelationship(GcpPubsubPublisher.REL_TOOBIG);
        results_tobig.forEach(new Consumer<MockFlowFile>() {
            @Override
            public void accept(MockFlowFile mockFlowFile) {
                String resultValue = mockFlowFile.toString();
                System.out.println("\n Match: " + resultValue);
                // Test attributes and content
                //result.assertAttributeEquals(NIFIQueryHasher.MATCH_ATTR, "nifi rocks");
                //System.out.println(mockFlowFile.getAttribute(NIFIQueryHasher.MATCH_ATTR));
                //result.assertContentEquals("nifi rocks");
            }
        });
    }
}