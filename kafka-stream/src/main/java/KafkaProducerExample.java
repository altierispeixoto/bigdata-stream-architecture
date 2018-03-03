import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Stream;


public class KafkaProducerExample {

    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS ="172.20.128.3:9092";


    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);


        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }


    static void runProducer(final int sendMessageCount) {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();


        try {
            //for (long index = time; index < time + sendMessageCount; index++) {

                Random gerador = new Random();


                //device_id;dtEvent;vel_inst,
                for(String event : CSVReader.readEventCsv().split("\n")){

                    System.out.println(event);

                    final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, time, event);
                    RecordMetadata metadata = producer.send(record).get();
                }




             /*   final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, time, event);

                RecordMetadata metadata = producer.send(record).get();

                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), elapsedTime);*/
          // }
        }catch (Exception e ){
            e.printStackTrace();
        }
        finally {
            producer.flush();
            producer.close();
        }
    }



    public static void main(String... args) throws Exception {
        if (args.length == 0) {

//            while (true) {
                runProducer(4);
//               Thread.sleep(2000);
//            }
        } else {
            runProducer(Integer.parseInt(args[0]));
        }
    }

}
