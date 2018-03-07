import br.com.ceabs.domain.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.streaming.EsSparkStreaming;
import scala.*;
import java.lang.Double;
import java.lang.Long;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;

/**
 *
 * Created by altieris on 02/03/18.
 *
 */
public class JavaStatefulNetworkWordCount{

    private static final String KINESISAPPNAME ="sparkKinesisIntegration";
    private static final String STREAMNAME = "integration-test";
    private static final String ENDPOINT="https://kinesis.us-west-2.amazonaws.com";
    private static final String CHECKPOINT_DIR="../docker/spark/data/output/checkpoint";
    private static final int CHECKPOINT_INTERVAL_MILLIS = 4000;
    private static final String KINESIS_REGION="us-west-2";


    // Spark Streaming batch interval
    private static Duration batchInterval = new Duration(CHECKPOINT_INTERVAL_MILLIS);
    private static Duration kinesisCheckpointInterval = new Duration(CHECKPOINT_INTERVAL_MILLIS);


    public static JavaStreamingContext createContext(String checkpointDirectory) throws Exception {

        // Setup the Spark config and StreamingContext
        SparkConf sparkConfig = new SparkConf()
                .setMaster("local[2]")
                .set("spark.sql.warehouse.dir","/home/altieris/spark")
                .set("spark.streaming.stopGracefullyOnShutdown","true")
                .set("spark.cleaner.referenceTracking.cleanCheckpoints","true")
                .set("es.index.auto.create", "true")
                .set(ConfigurationOptions.ES_NODES, "172.20.128.102")
                .set(ConfigurationOptions.ES_PORT, "9200")
                .set("es.batch.write.retry.count", "-1")
                .set("es.write.operation", "upsert")
                .set("es.mapping.id", "tripId")
                .setAppName("JavaKinesisWordCountASL");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);


        AmazonKinesis kinesisClient = getKinesisClient();

        // In this example, we're going to create 1 Kinesis Receiver/input DStream for each shard.
        // This is not a necessity; if there are less receivers/DStreams than the number of shards,
        // then the shards will be automatically distributed among the receivers and each receiver
        // will receive data from multiple shards.

        DescribeStreamResult describeStreamResult = kinesisClient.describeStream(STREAMNAME);

        int numShards = describeStreamResult.getStreamDescription().getShards().size();
        int numStreams = numShards;


        // Create the Kinesis DStreams
        JavaDStream<byte[]> kinesisStream = getStream(numStreams, jssc);


        /**
         * Transform byte[] from kinesis to Event.
         */
        JavaPairDStream<String,List<Event>> events = kinesisStream.mapToPair((PairFunction<byte[], String, List<Event>>) bytes -> {
            String s = new String(bytes, StandardCharsets.UTF_8);
            LineWithTimestamp line =  new LineWithTimestamp();
            line.setLine(s);
            Event evt = new Event(line);
            List<Event> evtL = new ArrayList<>();
            evtL.add(evt);
            return new Tuple2<>(evt.getDeviceSerialNumber(),evtL);

        }).reduceByKey((Function2<List<Event>, List<Event>, List<Event>>) (events1, events2) -> {
            events1.addAll(events2);
            return events1;
        });



        // DStream made of get cumulative counts that get updated in every batch
        final JavaMapWithStateDStream<String, List<Event>, TripInfo, TripUpdate> stateDstream =
                events.mapWithState(StateSpec.function(new Function3<String, Optional<List<Event>>, State<TripInfo>, TripUpdate>() {
                    @Override
                    public TripUpdate call(String deviceSerialNumber, Optional<List<Event>> optional, State<TripInfo> state) throws Exception {

                        if (state.isTimingOut()) {


                            TripUpdate finalUpdate = new TripUpdate(state.get().getTripId());

                            finalUpdate.setDeviceSerialNumber(deviceSerialNumber);
                            finalUpdate.setDurationMs(state.get().calculateDuration()); // TripDuration
                            finalUpdate.setNumEvents(state.get().getNumEvents());

                            finalUpdate.setSpeedMax(state.get().getSpeedMax());
                            finalUpdate.setSpeedMin(state.get().getSpeedMin());

                            finalUpdate.setSpeedAvg(state.get().calculateSpeedAvg());
                            finalUpdate.setStartTrip(state.get().getEventTripStart());
                            finalUpdate.setEndTrip(state.get().getEventTripEnd());
                            finalUpdate.setTripStatus("FINISHED");
                            finalUpdate.setExpired(true);
                            //state.remove();

                            return finalUpdate;

                        } else{

                            long maxEventTime = Long.MIN_VALUE;
                            long minEventTime = Long.MAX_VALUE;

                            int numNewEvents = 0;
                            double speedSum = 0;
                            double speedMax = Double.MIN_VALUE;
                            double speedMin = Double.MAX_VALUE;

                            List<Event> events = optional.get();

                            for(Event e :  events) {

                                long timestampEvent = e.getDtEvent().getTime();
                                double speed = e.getSpeed();

                                maxEventTime = Math.max(timestampEvent, maxEventTime);
                                minEventTime = Math.min(timestampEvent, minEventTime);

                                speedMax = Math.max(speed, speedMax);
                                speedMin = Math.min(speed, speedMin);

                                speedSum += e.getSpeed();
                                numNewEvents += 1;
                            }

                            TripInfo tripStaging = new TripInfo();

                            if(state.exists()){

                                TripInfo oldTrip = state.get();
                                tripStaging.setTripId(oldTrip.getTripId());
                                tripStaging.setNumEvents(oldTrip.getNumEvents() + numNewEvents);
                                tripStaging.setSumSpeed(oldTrip.getSumSpeed() + speedSum);
                                tripStaging.setSpeedMax(Math.max(oldTrip.getSpeedMax(),speedMax));
                                tripStaging.setSpeedMin(Math.min(oldTrip.getSpeedMin(), speedMin));

                                tripStaging.setEventTripStart(new Timestamp(Math.min(oldTrip.getEventTripStart().getTime(), minEventTime))); // Trip Start date
                                tripStaging.setEventTripEnd(new Timestamp(Math.max(oldTrip.getEventTripEnd().getTime(), maxEventTime)));     // Trip End date
                            }else{
                                tripStaging.setTripId(String.valueOf(UUID.randomUUID()));
                                tripStaging.setNumEvents(numNewEvents);
                                tripStaging.setSumSpeed(speedSum);
                                tripStaging.setEventTripStart(new Timestamp(minEventTime));
                                tripStaging.setEventTripEnd(new Timestamp(maxEventTime));
                                tripStaging.setSpeedMax(speedMax);
                                tripStaging.setSpeedMin(speedMin);
                            }

                            state.update(tripStaging);

                            TripUpdate tripUpdate = new TripUpdate(state.get().getTripId());
                            tripUpdate.setDeviceSerialNumber(deviceSerialNumber);
                            tripUpdate.setDurationMs(state.get().calculateDuration());
                            tripUpdate.setNumEvents(state.get().getNumEvents());
                            tripUpdate.setStartTrip(state.get().getEventTripStart());
                            tripUpdate.setEndTrip(state.get().getEventTripEnd());
                            tripUpdate.setSpeedMax(state.get().getSpeedMax());
                            tripUpdate.setSpeedMin(state.get().getSpeedMin());
                            tripUpdate.setSpeedAvg(state.get().calculateSpeedAvg());
                            tripUpdate.setExpired(false);
                            tripUpdate.setTripStatus("RUNNING");
                            return tripUpdate;
                        }
                    }
                }).timeout(Duration.apply(10000))); // put timout here


       final DStream<TripUpdate> tripFiltered = stateDstream.filter(tripUpdate -> tripUpdate.isExpired()).dstream();

        tripFiltered.print();
        EsSparkStreaming.saveToEs(tripFiltered, "spark/trip");

        jssc.checkpoint(checkpointDirectory);

        return jssc;
    }



    private static JavaDStream<byte[]> getStream(int numStreams, JavaStreamingContext jssc) {

        List<JavaDStream<byte[]>> streamsList = new ArrayList<>(numStreams);

        for (int i = 0; i < numStreams; i++) {
            streamsList.add(
                    KinesisUtils.createStream(jssc, KINESISAPPNAME, STREAMNAME, ENDPOINT, KINESIS_REGION,
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


        return unionStreams;
    }


    private static AmazonKinesis getKinesisClient() {

        AmazonKinesisClientBuilder kinesisClient  = AmazonKinesisClientBuilder.standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion(KINESIS_REGION);


        AmazonKinesis client = kinesisClient.build();
    /*    client.setEndpoint(ENDPOINT);*/

        System.out.println(client.describeStream(STREAMNAME));
        return client;
    }



    public static  void main(String[] args) throws Exception {
        Function0<JavaStreamingContext> createContextFunc =
                () -> createContext(CHECKPOINT_DIR);

        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(CHECKPOINT_DIR, createContextFunc);
        ssc.start();
        ssc.awaitTermination();

    }


}
