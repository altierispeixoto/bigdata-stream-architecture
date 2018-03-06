package br.com.ceabs.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.log4j.Log4j;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * User-defined data type representing the input events
 */

@Log4j
@ToString
@NoArgsConstructor
public class Event implements Serializable {

    @Getter
    @Setter
    private String deviceSerialNumber;

    @Getter
    @Setter
    private Timestamp dtEvent;

    @Getter @Setter
    private double speed;


    @Getter @Setter
    private Timestamp timestamp; // Timestamp event received on kafka



    public Event(String line) {

    }


    public Event(String deviceSerialNumber, Timestamp timestamp) {
        this.deviceSerialNumber = deviceSerialNumber;
        this.timestamp = timestamp;
    }

   public Event(LineWithTimestamp line){

       String evt[] = line.getLine().split(";");
       this.timestamp = line.getTimestamp(); //data e hora de chegada de recebimento do evento


       this.deviceSerialNumber = evt[0];
       this.dtEvent = convertStringToTimestamp(evt[1]);
       this.speed = new Double(evt[2]);

       log.info(this.deviceSerialNumber + " "+this.speed);
   }


    public static Timestamp convertStringToTimestamp(String str_date) {
        try {
            DateFormat formatter;
            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            // you can change format of date
            Date date = formatter.parse(str_date);
            Timestamp timeStampDate = new Timestamp(date.getTime());

            return timeStampDate;
        } catch (ParseException e) {
            System.out.println("Exception :" + e);
            return null;
        }
    }

}
