package br.com.ceabs.old;

import br.com.ceabs.domain.Event;
import br.com.ceabs.domain.Trip;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;


/**
 * https://spark.apache.org/docs/latest/streaming-programming-guide.html
 *
 * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
 *
 * https://cdn2.hubspot.net/hubfs/438089/Landing_pages/blog-books/Mastering-Apache-Spark-2.0.pdf
 *
 */

public class SparkKafkaConsumer {

    private static final String checkpointDirectory = "/tmp/data/output/log/log-event-streaming";


    private static JavaStreamingContext createContext(String checkpointDirectory){

        System.out.println("Creating new context");


        String topic = "test";
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topic));
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");

        JavaStreamingContext jssc =  new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, getKafkaProperties()));


        jssc.checkpoint(checkpointDirectory);


        // Get the events, generate k,v , compute trip

        JavaDStream<Event> eventsRDD = messages.map(
                (Function<ConsumerRecord<String, String>, Event>) v1 -> new Event(v1.value())
        );

        JavaPairDStream<String, List<Event>> eventPairs = eventsRDD.mapToPair((PairFunction<Event, String, List<Event>>) event -> {
            List<Event> events = new ArrayList<>();
            events.add(event);
            return new Tuple2<>(event.getDeviceSerialNumber(),events);
        });


        final JavaPairDStream<String, List<Event>> dStream = eventPairs.reduceByKey((Function2<List<Event>, List<Event>, List<Event>>) (v1, v2) -> {
            v1.addAll(v2);
            return v1;
        });


        //Atualiza o estado das viagens
        JavaPairDStream<String, Trip> tripStream = dStream.updateStateByKey(new UpdateTripStatus());
        tripStream.checkpoint(Duration.apply(5000));
        tripStream.print();


        //filtra viagens finalizadas
        JavaDStream<Trip> tripJavaDStream = tripStream.filter(new Function<Tuple2<String, Trip>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Trip> v1) throws Exception {
                return v1._2().isEndTrip();
            }
        }).map(new Function<Tuple2<String,Trip>, Trip>() {
            @Override
            public Trip call(Tuple2<String, Trip> v1) throws Exception {
                return v1._2();
            }
        });

        tripStream = tripStream.filter(new Function<Tuple2<String, Trip>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Trip> v1) throws Exception {
                return !v1._2().isEndTrip();
            }
        });

        tripJavaDStream.foreachRDD(new VoidFunction<JavaRDD<Trip>>() {
            @Override
            public void call(JavaRDD<Trip> tripJavaRDD) throws Exception {
                tripJavaRDD.saveAsTextFile("/tmp/data/output/trip-stream");
            }
        });




        return jssc;
    }

    public static  void main(String[] args) throws Exception {
        Function0<JavaStreamingContext> createContextFunc =
                () -> createContext(checkpointDirectory);

        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);
        ssc.start();
        ssc.awaitTermination();

    }

    public static Map<String,Object> getKafkaProperties(){

        String brokers ="172.19.0.3:9092";


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", LongDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "SPARK_CONSUMER");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        return kafkaParams;
    }
}
