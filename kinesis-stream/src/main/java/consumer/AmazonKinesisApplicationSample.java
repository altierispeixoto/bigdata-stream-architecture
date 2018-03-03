package consumer;


import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;


import java.net.InetAddress;
import java.util.UUID;

/**
 *
 * Created by altieris on 01/03/18.
 *
 */
public class AmazonKinesisApplicationSample {


    public static final String SAMPLE_APPLICATION_STREAM_NAME = "integration-test";

    private static final String SAMPLE_APPLICATION_NAME = "SampleKinesisApplication";

    // Initial position in the stream when the application starts up for the first time.
    // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private static final InitialPositionInStream SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM =
            InitialPositionInStream.LATEST;


    public static void main(String[] args) throws Exception {

        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();

        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(SAMPLE_APPLICATION_NAME,
                        SAMPLE_APPLICATION_STREAM_NAME,
                        new DefaultAWSCredentialsProviderChain(),
                        workerId);

        kinesisClientLibConfiguration.withInitialPositionInStream(SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM);
        kinesisClientLibConfiguration.withRegionName("us-west-2");

        IRecordProcessorFactory recordProcessorFactory = new AmazonKinesisApplicationRecordProcessorFactory();
        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

        System.out.printf("Running %s to process stream %s as worker %s...\n",
                SAMPLE_APPLICATION_NAME,
                SAMPLE_APPLICATION_STREAM_NAME,
                workerId);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            System.err.println("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }

/*
    public static void deleteResources() {
        // Delete the stream
        AmazonKinesis kinesis = AmazonKinesisClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        System.out.printf("Deleting the Amazon Kinesis stream used by the sample. Stream Name = %s.\n",
                SAMPLE_APPLICATION_STREAM_NAME);
        try {
            kinesis.deleteStream(SAMPLE_APPLICATION_STREAM_NAME);
        } catch (ResourceNotFoundException ex) {
            // The stream doesn't exist.
        }

        // Delete the table
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        System.out.printf("Deleting the Amazon DynamoDB table used by the Amazon Kinesis Client Library. Table Name = %s.\n",
                SAMPLE_APPLICATION_NAME);
        try {
            dynamoDB.deleteTable(SAMPLE_APPLICATION_NAME);
        } catch (com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException ex) {
            // The table doesn't exist.
        }
    }
*/

}
