package com.rideplatform.core.model;

import java.io.Serializable;

public class Driver implements Serializable {
    private static final long serialVersionUID = 1L;

    private String driverId;
    private double lat;
    private double lng;

    public Driver() {}

    public Driver(String driverId, double lat, double lng) {
        this.driverId = driverId;
        this.lat = lat;
        this.lng = lng;
    }

    // Getters and Setters
    public String getDriverId() { return driverId; }
    public void setDriverId(String driverId) { this.driverId = driverId; }

    public double getLat() { return lat; }
    public void setLat(double lat) { this.lat = lat; }

    public double getLng() { return lng; }
    public void setLng(double lng) { this.lng = lng; }
}
