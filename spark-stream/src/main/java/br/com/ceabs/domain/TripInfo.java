package br.com.ceabs.domain;


import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * User-defined data type for storing a session information as state in mapGroupsWithState.
 */
@NoArgsConstructor
public class TripInfo implements Serializable {

    @Getter
    @Setter
    private String tripId;

    @Getter
    @Setter
    private int numEvents = 0;

    @Getter @Setter
    private Double sumSpeed;

    @Getter @Setter
    private Double speedMax;

    @Getter @Setter
    private Double speedMin;

    @Getter @Setter
    private Timestamp eventTripStart;

    @Getter @Setter
    private Timestamp eventTripEnd;


    public TripInfo(String tripId){
        this.tripId = tripId;
    }

    public long calculateDuration() {
        return eventTripEnd.getTime() - eventTripStart.getTime();
    }

    public double calculateSpeedAvg() {
        return sumSpeed/numEvents;
    }

}