import br.com.ceabs.EventMapper;
import br.com.ceabs.TripStateUpdateFlatMap;
import br.com.ceabs.domain.Event;
import br.com.ceabs.domain.LineWithTimestamp;
import br.com.ceabs.domain.TripInfo;
import br.com.ceabs.domain.TripUpdate;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import scala.collection.JavaConversions;
import scala.collection.Seq;


import java.util.*;

/**
 * Created by altieris on 20/02/18.
 * <p>
 * https://docs.databricks.com/spark/latest/structured-streaming/index.html
 * http://asyncified.io/2017/07/30/exploring-stateful-streaming-with-spark-structured-streaming/
 * <p>
 * http://boss.dima.tu-berlin.de/media/BOSS17-Tutorial-spark.pdf
 * https://pt.slideshare.net/databricks/deep-dive-into-stateful-stream-processing-in-structured-streaming-by-tathagata-das
 *
 * https://databricks.com/blog/2017/05/08/event-time-aggregation-watermarking-apache-sparks-structured-streaming.html
 * https://databricks.com/blog/2017/10/17/arbitrary-stateful-processing-in-apache-sparks-structured-streaming.html
 * https://databricks.com/blog/2017/01/19/real-time-streaming-etl-structured-streaming-apache-spark-2-1.html
 *
 * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes
 *
 * https://github.com/jaceklaskowski/spark-structured-streaming-book/blob/master/spark-sql-streaming-OutputMode.adoc
 * https://github.com/jaceklaskowski/spark-structured-streaming-book/blob/master/spark-sql-streaming-KeyValueGroupedDataset-flatMapGroupsWithState.adoc
 *
 * https://indico.in2p3.fr/event/14490/contributions/56348/attachments/44374/54982/xldb.pdf
 *
 * http://apache-spark-user-list.1001560.n3.nabble.com/flatMapGroupsWithState-not-timing-out-spark-2-2-1-td30556.html
 *
 * https://jaceklaskowski.gitbooks.io/spark-structured-streaming/spark-structured-streaming.html
 */
public final class JavaStructuredSessionization {

    public static void main(String[] args) throws Exception {


        String host = "172.20.128.3:9092";

        SparkConf config = new SparkConf()
                .set("spark.app.name", "spark_streaming")
                .set("spark.sql.warehouse.dir", "/tmp/data/output/")
                .set("spark.sql.streaming.checkpointLocation", "/tmp/data/output/checkpoint")
               // .set("spark.sql.shuffle.partitions", "2")
                .set("spark.speculation","false")
                .set("spark.streaming.receiver.writeAheadLog.enable","true")
                .set(ConfigurationOptions.ES_NODES, "172.20.128.102")
                .set(ConfigurationOptions.ES_PORT, "9200")
                .set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "true")
                //.set("es.net.ssl","true")
                //.set("es.nodes.wan.only","true")
                .set("es.batch.write.retry.count", "-1")
                .set("es.write.operation", "upsert")
                .set("es.mapping.id", "tripId")
                ;

        SparkSession spark = SparkSession.builder().config(config).getOrCreate();

        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", host)
                .option("subscribe", "test")
                .option("includeTimestamp", true)
                .load();

        // Split the lines into words, treat words as sessionId of events
        Dataset<LineWithTimestamp> lineWithTimestampDataset = streamToLineWithTimestamp(lines);
        Dataset<Event> events = lineWithTimestampDataset.map(new EventMapper(), Encoders.bean(Event.class));

        events.printSchema();

/*
        |-- deviceSerialNumber: string (nullable = true)
        |-- dtEvent: timestamp (nullable = true)
        |-- speed: integer (nullable = true)
        |-- timestamp: timestamp (nullable = true)*/


        Seq<String> uniqueKeys = JavaConversions.asScalaBuffer(Arrays.asList("deviceSerialNumber","dtEvent")).seq();


        // Step 2: Apply the trip update function to the events streaming Dataset grouped by deviceSerialNumber
        Dataset<TripUpdate> sessionUpdates = events
                .withWatermark("dtEvent", "2 minutes") // drop events late more than 10 minutes
                .dropDuplicates(uniqueKeys)
                .groupByKey(
                        (MapFunction<Event, String>) Event::getDeviceSerialNumber, Encoders.STRING()
                ).flatMapGroupsWithState(
                        new TripStateUpdateFlatMap(),
                        OutputMode.Append(),
                        Encoders.bean(TripInfo.class),
                        Encoders.bean(TripUpdate.class),
                        GroupStateTimeout.EventTimeTimeout()
                );

               /* .mapGroupsWithState(
                        new TripStateUpdate(), Encoders.bean(TripInfo.class), Encoders.bean(TripUpdate.class),
                        GroupStateTimeout.EventTimeTimeout()
                );*///.filter("expired = true");

        sessionUpdates.printSchema();

  /*      *
         root
         |-- deviceSerialNumber: string (nullable = true)
         |-- durationMs: long (nullable = true)
         |-- expired: boolean (nullable = true)
         |-- numEvents: integer (nullable = true)
         |-- speedAvg: double (nullable = true)
         |-- speedMax: double (nullable = true)
         |-- speedMin: double (nullable = true)*/

//        StreamingQuery query = sessionUpdates.
//                .writeStream()
//                .outputMode(OutputMode.Append())
//                .format("csv")
//                .option("path", "/tmp/data/output/trip-stream/")
//                //.option("checkpointLocation", "/tmp/data/output/checkpoint/")
//                .queryName("trips")
//                .trigger(Trigger.ProcessingTime("5 seconds"))
//                .start();





    /*Exception in thread "main" java.lang.UnsupportedOperationException: Data source jdbc does not support streamed writing*/
/*    StreamingQuery query = sessionUpdates
                .writeStream()
                .outputMode(OutputMode.Append())
                .format("jdbc")
                //.option("path", "/tmp/data/output/trip-stream/")
                //.option("checkpointLocation", "/tmp/data/output/checkpoint/")
                .queryName("trips")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start("jdbc:mysql://mysql:3306/test");*/

/*

        *
         * Output Mode
         *
         * Complete - Output whole answer every time
         * Update   - Ouput changed rows
         * Append   - Output new rows only*/


        StreamingQuery query = sessionUpdates
                .writeStream()
                //.outputMode(OutputMode.Append())
                //.option("checkpointLocation", "/tmp/data/output/checkpoint/trip")
                .format("org.elasticsearch.spark.sql")
                //.option("es.mapping.id", "tripId")
                //.option("es.write.operation", "update")
                .start("spark/trip");


        // Start running the query that prints the trip updates to the console -- OUTPUT CONSOLE WORKING
        StreamingQuery query1 = sessionUpdates
                .writeStream()
                .outputMode(OutputMode.Append())
                //.option("path", "/tmp/data/output/trip-stream/json-sink")
                //.option("checkpointLocation", "/tmp/data/output/checkpoint/")
                .format("console")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();


        query1.awaitTermination();
        query.awaitTermination();
    }

    private static Dataset<LineWithTimestamp>  streamToLineWithTimestamp(Dataset<Row> lines){
        return lines
                .withColumnRenamed("value", "line")
                .selectExpr("CAST(line AS STRING)", "CAST(timestamp as TIMESTAMP)")
                .as(Encoders.bean(LineWithTimestamp.class));
    }
}

