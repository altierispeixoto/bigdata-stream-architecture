package br.com.ceabs.domain;


import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * User-defined data type representing the update information returned by mapGroupsWithState.
 */
@NoArgsConstructor
@ToString
public class TripUpdate implements Serializable {


    @Getter
    @Setter
    private String tripId;


    @Getter
    @Setter
    private String deviceSerialNumber;

    @Getter @Setter
    private long durationMs;

    @Getter @Setter
    private int numEvents;

    @Getter @Setter
    private boolean expired;

    @Getter @Setter
    private double speedMax;

    @Getter @Setter
    private double speedMin;

    @Getter @Setter
    private double speedAvg;

    @Getter @Setter
    private Timestamp startTrip;

    @Getter @Setter
    private Timestamp endTrip;

    @Getter @Setter
    private String tripStatus;

    @Getter @Setter
    private Double sumSpeed;

    public TripUpdate(String tripId){
        this.tripId = tripId;
    }

    public TripUpdate(String deviceSerialNumber, long durationMs, int numEvents, boolean expired) {
        this.deviceSerialNumber = deviceSerialNumber;
        this.durationMs = durationMs;
        this.numEvents = numEvents;
        this.expired = expired;

    }
}