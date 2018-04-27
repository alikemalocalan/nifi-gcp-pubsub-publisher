package com.synack.nifi.gcp.pubsub.publisher;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

public class GcpPubsubPublisherTest {


    @Test
    public void sendStringToPubSub() {

        //content of credential json file
        String authkey = "";

        // Content to be mock a json file
        InputStream content = new ByteArrayInputStream("1523531787267\t_ip=46.221.145.21&module=browse&catCode=jv&format=1%2C2%2C3%2C4&orderBy=wbm&pageNum=1&resultType=catalog&totfound=3&contentName=SRP%3ABrowse&abvar=BMF5&source=zf&sid=rnbajnob7017eine32kh5ji9p2&agent=Mozilla%2F5.0+%28Linux%3B+Android+6.0.1%3B+SM-G925F+Build%2FMMB29K%3B+wv%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko%29+Version%2F4.0+Chrome%2F65.0.3325.109+Mobile+Safari%2F537.36&referer=https%3A%2F%2Fwww.gittigidiyor.com%2Fbesin-takviyesi%3FSpec%3D%5BMarka%7EEnterogermina%5D".getBytes());
        //InputStream content2 = new ByteArrayInputStream("1509897759817   _bot=0&_c=3aa8167f8129905d2fad17c1b653f8467808bafb48f8885d37574f2bd0b3f5a6&_ip=b68caee6961e030b5f1819768608ee72d448f6326ed9c35cfbb64898ba366705&_t=mdev&ids=3d43beb3f819ebbfd56cd33346f659bab7849adaafcfabaa1e9491c590b6f770%2C8a2d98e2a0e6b4507c93fa6ad0531db47340f416cc9111a3b94720be36c99360%2C3fedd6b57f31badf2975b39181a1f18ff7511891dcfffcaf48ad62b97414c08f%2Ce07912e577702d0a43f4f2262baea1435837a87fd61d2774b8654a92fa4dd621%2C74b889a72b2467f77f1f982863a9e4e62108bebf3b1bf666398b4b962162bd5d%2Cd92564ecea83b9a56cb3e3d33b38bce56777e41810302eec00bd7648ee8a48e8%2C42264ff3896e6a812390d204ed07c39d76f410031108a626424df9e76ecbd5e3%2Cc7ac5ea76c7f75d10826f39a410f8a29e3acfd40a1cbebfa6735507188bbae47%2C3990c42a8a677ca7fa7488ad81ec8677b5a3db2079352f3ed02615c106a93f9a%2C919a0df8bbce2d585a495b03c0b9bf2605bf4b31bce1ca4b38c115ec3cef5b9b%2C962a68ea0f4b8784d136f6ab54dc725e83aaf17daf6b82aa8eac681d41af72bc%2Ce80bb5090e0206b22f847826c20b748dff9ed30e5ad4dad1571e50e67ee7d643%2C18a0554bb07376e713745602e69bbbf7727cf35bbf8d95206ad30e756fe724bd%2Ca292d179bde95b190e59027105a64f48412d22cb92be2a90d4ce7d7e1a3d1866%2C5f869b217ae9d5f8720f715a6264b712b7811fcef941999ad841e5962f9fcf80%2C1e52d2f0dbf377b0d8e872510af662226a6bb9dbf978364b47f9f2eaa3b1b56c%2C3783d39863dca60668c3ed70623136bf5c5a7b62a35c9cedfc261df979c3239e%2C20736792b781a31c3f4377af71222775e3cfadfa224132134295057873922927%2C4f37ec0a3ceaa746055cadfb374b49e7423d029ae1f45cab1f0dfc7f463af277%2Ce47fbce426c04650bbe7d65d792c5d5ca7e5f2ae6c02d43725b958caebb50220&keyword=fonksiyonlu+saat&module=search&orderBy=PD&pageNum=8&sid=0000a448b5d3595d5c5a2e6562cc510d5b6585a4&totfound=439".getBytes());
        TestRunner testRunner = TestRunners.newTestRunner(GcpPubsubPublisher.class);

        // Generate a test runner to mock a processor in a flow
        testRunner.setProperty(GcpPubsubPublisher.projectIdProperty, "gittigidiyor-test");
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