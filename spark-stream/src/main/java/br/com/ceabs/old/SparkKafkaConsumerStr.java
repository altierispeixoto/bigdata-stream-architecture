package br.com.ceabs.old;

import br.com.ceabs.domain.Event;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 *
 * Created by altieris on 20/02/18.
 *
 */
public class SparkKafkaConsumerStr {


    public static void main(String[] args) throws Exception {

        String brokers = "172.19.0.3:9092";


        SparkConf config = new SparkConf().set("spark.app.name", "spark_streaming")
                .set("spark.sql.warehouse.dir", "/tmp/data/output/")
                .set("spark.kryo.registrator", "StreamKryoRegistrator.class");


        SparkSession spark = SparkSession.builder().config(config).getOrCreate();


        Dataset<Row> eventRows = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("subscribe", "test")
                .load();
//
//
//         Dataset<Event> eventRDD = eventRows.selectExpr("CAST(value AS STRING)").map((MapFunction<Row, Event>) value -> {
//            String row1 = value.getString(0);
//            return new Event(row1);
//        }, Encoders.bean(Event.class));
//



//        StreamingQuery query = eventRDD.writeStream()
//                .outputMode("append")
//                .option("checkpointLocation", "/tmp/data/output/checkpoint")
//                .format("console")
//                .start();


//        query.awaitTermination();
    }

}
