import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 *
 * Created by altieris on 02/03/18.
 *
 */
public class JavaStatefulNetworkWordCount{


    public static void main(String[] args) throws Exception {

        // Populate the appropriate variables from the given args
        String kinesisAppName = "sparkKinesisIntegration";
        String streamName = "integration-test";
        String endpointUrl = "https://kinesis.us-west-2.amazonaws.com";


        // Create a Kinesis client in order to determine the number of shards for the given stream
/*
        AmazonKinesisClient kinesisClient =
                new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
*/


        BasicAWSCredentials awsCreds = new BasicAWSCredentials("", "");

        AmazonKinesisClient kinesisClient =
                new AmazonKinesisClient(new AWSStaticCredentialsProvider(awsCreds));


        kinesisClient.setEndpoint(endpointUrl);
        System.out.println(kinesisClient.describeStream(streamName));

        int numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size();


        // In this example, we're going to create 1 Kinesis Receiver/input DStream for each shard.
        // This is not a necessity; if there are less receivers/DStreams than the number of shards,
        // then the shards will be automatically distributed among the receivers and each receiver
        // will receive data from multiple shards.
        int numStreams = numShards;

        // Spark Streaming batch interval
        Duration batchInterval = new Duration(2000);

        // Kinesis checkpoint interval.  Same as batchInterval for this example.
        Duration kinesisCheckpointInterval = batchInterval;

        // Get the region name from the endpoint URL to save Kinesis Client Library metadata in
        // DynamoDB of the same region as the Kinesis stream
        String regionName = "us-west-2";//KinesisExampleUtils.getRegionNameByEndpoint(endpointUrl);

        // Setup the Spark config and StreamingContext
        SparkConf sparkConfig = new SparkConf().setAppName("JavaKinesisWordCountASL");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);

        // Create the Kinesis DStreams
        List<JavaDStream<byte[]>> streamsList = new ArrayList<>(numStreams);
        for (int i = 0; i < numStreams; i++) {
            streamsList.add(
                    KinesisUtils.createStream(jssc, kinesisAppName, streamName, endpointUrl, regionName,
                            InitialPositionInStream.LATEST, kinesisCheckpointInterval,
                            StorageLevel.MEMORY_AND_DISK_2())
            );
        }

        // Union all the streams if there is more than 1 stream
        JavaDStream<byte[]> unionStreams;
        if (streamsList.size() > 1) {
            unionStreams = jssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
        } else {
            // Otherwise, just use the 1 stream
            unionStreams = streamsList.get(0);
        }

        // Convert each line of Array[Byte] to String, and split into words
        JavaDStream<String> words = unionStreams.flatMap(new FlatMapFunction<byte[], String>() {
            @Override
            public Iterator<String> call(byte[] line) {
                String s = new String(line, StandardCharsets.UTF_8);
                System.out.println(s);
                return Arrays.asList(s).iterator();
            }
        });

        // Map each word to a (word, 1) tuple so we can reduce by key to count the words
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );

        // Print the first 10 wordCounts
        wordCounts.print();

        // Start the streaming context and await termination
        jssc.start();
        jssc.awaitTermination();
    }
}
