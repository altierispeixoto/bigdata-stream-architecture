package produtor;
/**
 *
 * Created by altieris on 01/03/18.
 *
 */

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescription;
import consumer.AmazonKinesisApplicationSample;

import static org.apache.commons.lang.CharEncoding.UTF_8;


public class AmazonKinesisRecordProducerSample {


    private static AmazonKinesis kinesis;

    public static void main(String[] args) throws Exception {


        kinesis = AmazonKinesisClientBuilder.standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion("us-west-2")
                .build();

        final String myStreamName = AmazonKinesisApplicationSample.SAMPLE_APPLICATION_STREAM_NAME;
        final Integer myStreamSize = 1;

        // Describe the stream and check if it exists.
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest()
                .withStreamName(myStreamName);


        try {

            StreamDescription streamDescription = kinesis.describeStream(describeStreamRequest).getStreamDescription();
            System.out.printf("Stream %s has a status of %s.\n", myStreamName, streamDescription.getStreamStatus());

            if ("DELETING".equals(streamDescription.getStreamStatus())) {
                System.out.println("Stream is being deleted. This sample will now exit.");
                System.exit(0);
            }

            // Wait for the stream to become active if it is not yet ACTIVE.
            if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
                waitForStreamToBecomeAvailable(myStreamName);
            }

        } catch (ResourceNotFoundException ex) {
            System.out.printf("Stream %s does not exist. Creating it now.\n", myStreamName);

            // Create a stream. The number of shards determines the provisioned throughput.
            CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(myStreamName);
            createStreamRequest.setShardCount(myStreamSize);
            kinesis.createStream(createStreamRequest);
            // The stream is now being created. Wait for it to become active.
            waitForStreamToBecomeAvailable(myStreamName);
        }
        // List all of my streams.
        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(10);
        ListStreamsResult listStreamsResult = kinesis.listStreams(listStreamsRequest);
        List<String> streamNames = listStreamsResult.getStreamNames();
        while (listStreamsResult.isHasMoreStreams()) {
            if (streamNames.size() > 0) {
                listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));
            }

            listStreamsResult = kinesis.listStreams(listStreamsRequest);
            streamNames.addAll(listStreamsResult.getStreamNames());
        }
        // Print all of my streams.
        System.out.println("List of my streams: ");
        for (int i = 0; i < streamNames.size(); i++) {
            System.out.println("\t- " + streamNames.get(i));
        }
        System.out.printf("Putting records in stream : %s until this application is stopped...\n", myStreamName);
        System.out.println("Press CTRL-C to stop.");


       // while (true) {
            long createTime = System.currentTimeMillis();
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(myStreamName);


            putRecordRequest.setData(ByteBuffer.wrap(String.format("6;2018-02-27 00:15:00.0;20").getBytes(UTF_8)));
            putRecordRequest.setPartitionKey(String.format("partitionKey-%d", createTime));

            PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
            System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
                    putRecordRequest.getPartitionKey(),
                    putRecordResult.getShardId(),
                    putRecordResult.getSequenceNumber());
     //   }
    }

    private static void waitForStreamToBecomeAvailable(String myStreamName) throws InterruptedException {
        System.out.printf("Waiting for %s to become ACTIVE...\n", myStreamName);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));

            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(myStreamName);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesis.describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                System.out.printf("\t- current state: %s\n", streamStatus);
                if ("ACTIVE".equals(streamStatus)) {
                    return;
                }
            } catch (ResourceNotFoundException ex) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
            } catch (AmazonServiceException ase) {
                throw ase;
            }
        }

        throw new RuntimeException(String.format("Stream %s never became active", myStreamName));
    }
}
