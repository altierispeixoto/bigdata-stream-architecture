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

    private float speedMax;

    private boolean endTrip;

    public String getDeviceSerialNumber() {
        return deviceSerialNumber;
    }

    public void setDeviceSerialNumber(String deviceSerialNumber) {
        this.deviceSerialNumber = deviceSerialNumber;
    }

    public float getSpeedMax() {
        return speedMax;
    }

    public void setSpeedMax(float speedAvg) {
        this.speedMax = speedAvg;
    }

    public boolean isEndTrip() {
        return endTrip;
    }

    public void setEndTrip(boolean endTrip) {
        this.endTrip = endTrip;
    }
}
