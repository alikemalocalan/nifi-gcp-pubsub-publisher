package com.synack.nifi.gcp.pubsub.publisher;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.List;

public class GcpPubsubPublisherTest {


    @Test
    public void sendStringToPubSub() {


        TestRunner testRunner = TestRunners.newTestRunner(GcpPubsubPublisher.class);


        //content of credential json file
        String authkey = "";

        // Generate a test runner to mock a processor in a flow
        testRunner.setProperty(GcpPubsubPublisher.projectIdProperty, "project name");
        testRunner.setProperty(GcpPubsubPublisher.topicProperty, "nifi-test-topic");
        testRunner.setProperty(GcpPubsubPublisher.authProperty, authkey);
        testRunner.setProperty(GcpPubsubPublisher.batchProperty, "5");

        // Add the content to the runner
        testRunner.enqueue("Test data");
        testRunner.enqueue("Test data");
        testRunner.enqueue("Test data");
        testRunner.enqueue("Test data");
        testRunner.enqueue("Test data");
        testRunner.enqueue("Test data");
        testRunner.enqueue("Test data");

        // Run the enqueued content, it also takes an int = number of contents queued
        testRunner.run();

        // All results were processed with out failure
        //testRunner.assertQueueEmpty();

        // If you need to read or do aditional tests on results you can access the content
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(GcpPubsubPublisher.SUCCESS);
        results.forEach(mockFlowFile -> {
            String resultValue = mockFlowFile.toString();
            System.out.println("\n Match: " + resultValue);
            // Test attributes and content
            //result.assertAttributeEquals(NIFIQueryHasher.MATCH_ATTR, "nifi rocks");
            //System.out.println(mockFlowFile.getAttribute(NIFIQueryHasher.MATCH_ATTR));
            //result.assertContentEquals("nifi rocks");
        });

        List<MockFlowFile> results_tobig = testRunner.getFlowFilesForRelationship(GcpPubsubPublisher.REL_TOOBIG);
        results_tobig.forEach(mockFlowFile -> {
            String resultValue = mockFlowFile.toString();
            System.out.println("\n Match: " + resultValue);
            // Test attributes and content
            //result.assertAttributeEquals(NIFIQueryHasher.MATCH_ATTR, "nifi rocks");
            //System.out.println(mockFlowFile.getAttribute(NIFIQueryHasher.MATCH_ATTR));
            //result.assertContentEquals("nifi rocks");
        });
    }
}