package br.com.ceabs.domain;

import lombok.ToString;

import java.io.Serializable;

/**
 *
 * Created by altieris on 19/02/18.
 *
 */
@ToString
public class Trip implements Serializable {

    private String deviceSerialNumber;

    private double speedMax;

    private boolean endTrip;

    public String getDeviceSerialNumber() {
        return deviceSerialNumber;
    }

    public void setDeviceSerialNumber(String deviceSerialNumber) {
        this.deviceSerialNumber = deviceSerialNumber;
    }

    public double getSpeedMax() {
        return speedMax;
    }

    public void setSpeedMax(double speedAvg) {
        this.speedMax = speedAvg;
    }

    public boolean isEndTrip() {
        return endTrip;
    }

    public void setEndTrip(boolean endTrip) {
        this.endTrip = endTrip;
    }
}
